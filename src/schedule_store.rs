// schedule_store.rs
// Durable persistence for schedules, generation counters, dependencies,
// and consumed edges (consume-once across restart).
//
// Backend: SQLite (sqlx). Survives process restarts.
//
// Tables:
//   schedules     – registered AgentSchedule definitions
//   generations   – per-agent successful completion counts
//   dependencies  – agent → depends_on + min_generation
//   consumed      – consumer → producer → last_consumed_generation

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid dependency: {0}")]
    InvalidDependency(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct StoredSchedule {
    pub id: String,
    pub agent_id: String,
    pub group_name: Option<String>,
    pub kind: String,
    pub expression: String,
    pub max_concurrent: i64,
    pub enabled: bool,
    pub wait_timeout_secs: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct GenerationState {
    pub agent_id: String,
    pub completed_count: i64,
    pub last_success_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DependencyRow {
    pub agent_id: String,
    pub depends_on: String,
    pub min_generation: i64,
}

pub struct ScheduleStore {
    pool: SqlitePool,
}

impl ScheduleStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let url = format!("sqlite:{}?mode=rwc", path.as_ref().display());
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS schedules (
                id                  TEXT PRIMARY KEY,
                agent_id            TEXT NOT NULL,
                group_name          TEXT,
                kind                TEXT NOT NULL,
                expression          TEXT NOT NULL,
                max_concurrent      INTEGER NOT NULL DEFAULT 1,
                enabled             INTEGER NOT NULL DEFAULT 1,
                wait_timeout_secs   INTEGER NOT NULL DEFAULT 300,
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS generations (
                agent_id            TEXT PRIMARY KEY,
                completed_count     INTEGER NOT NULL DEFAULT 0,
                last_success_at     TEXT
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                agent_id            TEXT NOT NULL,
                depends_on          TEXT NOT NULL,
                min_generation      INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (agent_id, depends_on)
            );

            CREATE TABLE IF NOT EXISTS consumed (
                consumer_agent_id   TEXT NOT NULL,
                producer_agent_id   TEXT NOT NULL,
                last_generation     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (consumer_agent_id, producer_agent_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sched_agent ON schedules(agent_id);
            CREATE INDEX IF NOT EXISTS idx_dep_agent   ON dependencies(agent_id);
            CREATE INDEX IF NOT EXISTS idx_consumed_consumer ON consumed(consumer_agent_id);
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn upsert_schedule(&self, s: &StoredSchedule) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            INSERT INTO schedules
                (id, agent_id, group_name, kind, expression, max_concurrent,
                 enabled, wait_timeout_secs, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                agent_id          = excluded.agent_id,
                group_name        = excluded.group_name,
                kind              = excluded.kind,
                expression        = excluded.expression,
                max_concurrent    = excluded.max_concurrent,
                enabled           = excluded.enabled,
                wait_timeout_secs = excluded.wait_timeout_secs,
                updated_at        = excluded.updated_at
            "#,
        )
        .bind(&s.id)
        .bind(&s.agent_id)
        .bind(&s.group_name)
        .bind(&s.kind)
        .bind(&s.expression)
        .bind(s.max_concurrent)
        .bind(s.enabled)
        .bind(s.wait_timeout_secs)
        .bind(s.created_at)
        .bind(s.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_enabled(&self) -> Result<Vec<StoredSchedule>, StoreError> {
        let rows = sqlx::query_as::<_, StoredSchedule>(
            "SELECT * FROM schedules WHERE enabled = 1 ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_schedule(&self, id: &str) -> Result<StoredSchedule, StoreError> {
        sqlx::query_as::<_, StoredSchedule>("SELECT * FROM schedules WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM schedules WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_completed(&self, agent_id: &str) -> Result<u64, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT completed_count FROM generations WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0 as u64).unwrap_or(0))
    }

    pub async fn mark_success(&self, agent_id: &str) -> Result<u64, StoreError> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO generations (agent_id, completed_count, last_success_at)
            VALUES (?, 1, ?)
            ON CONFLICT(agent_id) DO UPDATE SET
                completed_count = completed_count + 1,
                last_success_at = excluded.last_success_at
            "#,
        )
        .bind(agent_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.get_completed(agent_id).await
    }

    pub async fn add_dependency(
        &self,
        agent_id: &str,
        depends_on: &str,
        min_generation: u64,
    ) -> Result<(), StoreError> {
        if agent_id == depends_on {
            return Err(StoreError::InvalidDependency(
                "self-dependency not allowed".into(),
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO dependencies (agent_id, depends_on, min_generation)
            VALUES (?, ?, ?)
            ON CONFLICT(agent_id, depends_on) DO UPDATE SET
                min_generation = excluded.min_generation
            "#,
        )
        .bind(agent_id)
        .bind(depends_on)
        .bind(min_generation as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_dependencies(
        &self,
        agent_id: &str,
    ) -> Result<HashMap<String, u64>, StoreError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT depends_on, min_generation FROM dependencies WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(k, v)| (k, v as u64)).collect())
    }

    pub async fn remove_dependency(
        &self,
        agent_id: &str,
        depends_on: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "DELETE FROM dependencies WHERE agent_id = ? AND depends_on = ?",
        )
        .bind(agent_id)
        .bind(depends_on)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn dependencies_satisfied(&self, agent_id: &str) -> Result<bool, StoreError> {
        let deps = self.get_dependencies(agent_id).await?;
        for (dep, min_gen) in deps {
            let done = self.get_completed(&dep).await?;
            if done < min_gen {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn set_consumed(
        &self,
        consumer_agent_id: &str,
        producer_agent_id: &str,
        generation: u64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            INSERT INTO consumed (consumer_agent_id, producer_agent_id, last_generation)
            VALUES (?, ?, ?)
            ON CONFLICT(consumer_agent_id, producer_agent_id) DO UPDATE SET
                last_generation = CASE
                    WHEN excluded.last_generation > last_generation
                    THEN excluded.last_generation
                    ELSE last_generation
                END
            "#,
        )
        .bind(consumer_agent_id)
        .bind(producer_agent_id)
        .bind(generation as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_consumed(
        &self,
        consumer_agent_id: &str,
        producer_agent_id: &str,
    ) -> Result<u64, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT last_generation FROM consumed
            WHERE consumer_agent_id = ? AND producer_agent_id = ?
            "#,
        )
        .bind(consumer_agent_id)
        .bind(producer_agent_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0 as u64).unwrap_or(0))
    }

    pub async fn load_all_consumed(
        &self,
    ) -> Result<HashMap<String, HashMap<String, u64>>, StoreError> {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT consumer_agent_id, producer_agent_id, last_generation FROM consumed",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out: HashMap<String, HashMap<String, u64>> = HashMap::new();
        for (consumer, producer, gen) in rows {
            out.entry(consumer)
                .or_default()
                .insert(producer, gen as u64);
        }
        Ok(out)
    }
}
