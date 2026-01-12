//! HPC Job Scheduler Integration
//!
//! Provides integration with HPC job schedulers like Slurm and PBS/Torque.
//! Allows SmartCopy to be run as scheduled jobs and report progress.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Detected HPC scheduler type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerType {
    /// Slurm Workload Manager
    Slurm,
    /// PBS/Torque
    Pbs,
    /// PBS Pro
    PbsPro,
    /// LSF (IBM Spectrum LSF)
    Lsf,
    /// SGE (Sun Grid Engine) / UGE
    Sge,
    /// No scheduler detected
    None,
}

impl SchedulerType {
    /// Detect scheduler from environment
    pub fn detect() -> Self {
        // Check for Slurm
        if env::var("SLURM_JOB_ID").is_ok() || which("srun").is_some() {
            return SchedulerType::Slurm;
        }

        // Check for PBS
        if env::var("PBS_JOBID").is_ok() || which("qsub").is_some() {
            if env::var("PBS_O_WORKDIR").is_ok() {
                return SchedulerType::Pbs;
            }
            // Check if it's PBS Pro
            if let Some(output) = run_command("qstat", &["--version"]) {
                if String::from_utf8_lossy(&output.stdout).contains("pbs_version") {
                    return SchedulerType::PbsPro;
                }
            }
            return SchedulerType::Pbs;
        }

        // Check for LSF
        if env::var("LSB_JOBID").is_ok() || which("bsub").is_some() {
            return SchedulerType::Lsf;
        }

        // Check for SGE/UGE
        if env::var("SGE_TASK_ID").is_ok() || which("qsub").is_some() {
            if env::var("SGE_ROOT").is_ok() {
                return SchedulerType::Sge;
            }
        }

        SchedulerType::None
    }

    /// Get scheduler name
    pub fn name(&self) -> &'static str {
        match self {
            SchedulerType::Slurm => "Slurm",
            SchedulerType::Pbs => "PBS/Torque",
            SchedulerType::PbsPro => "PBS Pro",
            SchedulerType::Lsf => "IBM Spectrum LSF",
            SchedulerType::Sge => "SGE/UGE",
            SchedulerType::None => "None",
        }
    }
}

/// HPC job information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    /// Scheduler type
    pub scheduler: SchedulerType,
    /// Job ID
    pub job_id: Option<String>,
    /// Job name
    pub job_name: Option<String>,
    /// Number of nodes allocated
    pub num_nodes: Option<u32>,
    /// Number of tasks/processes
    pub num_tasks: Option<u32>,
    /// CPUs per task
    pub cpus_per_task: Option<u32>,
    /// Memory allocated (bytes)
    pub memory: Option<u64>,
    /// Time limit (seconds)
    pub time_limit: Option<u64>,
    /// Working directory
    pub work_dir: Option<PathBuf>,
    /// Node list
    pub node_list: Vec<String>,
    /// Job array info
    pub array_info: Option<ArrayInfo>,
}

/// Job array information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayInfo {
    /// Array job ID
    pub array_job_id: String,
    /// Task ID within array
    pub task_id: u32,
    /// Total tasks in array
    pub task_count: Option<u32>,
}

impl JobInfo {
    /// Collect job information from environment
    pub fn collect() -> Self {
        let scheduler = SchedulerType::detect();

        match scheduler {
            SchedulerType::Slurm => Self::collect_slurm(),
            SchedulerType::Pbs | SchedulerType::PbsPro => Self::collect_pbs(),
            SchedulerType::Lsf => Self::collect_lsf(),
            SchedulerType::Sge => Self::collect_sge(),
            SchedulerType::None => Self::empty(),
        }
    }

    fn empty() -> Self {
        Self {
            scheduler: SchedulerType::None,
            job_id: None,
            job_name: None,
            num_nodes: None,
            num_tasks: None,
            cpus_per_task: None,
            memory: None,
            time_limit: None,
            work_dir: None,
            node_list: Vec::new(),
            array_info: None,
        }
    }

    fn collect_slurm() -> Self {
        let job_id = env::var("SLURM_JOB_ID").ok();
        let job_name = env::var("SLURM_JOB_NAME").ok();
        let num_nodes = env::var("SLURM_NNODES").ok().and_then(|s| s.parse().ok());
        let num_tasks = env::var("SLURM_NTASKS").ok().and_then(|s| s.parse().ok());
        let cpus_per_task = env::var("SLURM_CPUS_PER_TASK").ok().and_then(|s| s.parse().ok());
        let work_dir = env::var("SLURM_SUBMIT_DIR").ok().map(PathBuf::from);

        // Parse memory (can be in various formats: 1G, 1024M, etc.)
        let memory = env::var("SLURM_MEM_PER_NODE").ok().and_then(parse_memory);

        // Parse time limit (format: D-HH:MM:SS or HH:MM:SS)
        let time_limit = env::var("SLURM_TIMELIMIT").ok().and_then(parse_slurm_time);

        // Parse node list
        let node_list = env::var("SLURM_NODELIST")
            .ok()
            .map(|s| expand_node_list(&s))
            .unwrap_or_default();

        // Array job info
        let array_info = env::var("SLURM_ARRAY_JOB_ID").ok().map(|array_job_id| {
            ArrayInfo {
                array_job_id,
                task_id: env::var("SLURM_ARRAY_TASK_ID")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                task_count: env::var("SLURM_ARRAY_TASK_COUNT")
                    .ok()
                    .and_then(|s| s.parse().ok()),
            }
        });

        Self {
            scheduler: SchedulerType::Slurm,
            job_id,
            job_name,
            num_nodes,
            num_tasks,
            cpus_per_task,
            memory,
            time_limit,
            work_dir,
            node_list,
            array_info,
        }
    }

    fn collect_pbs() -> Self {
        let job_id = env::var("PBS_JOBID").ok();
        let job_name = env::var("PBS_JOBNAME").ok();
        let work_dir = env::var("PBS_O_WORKDIR").ok().map(PathBuf::from);

        // Parse node file
        let node_list = env::var("PBS_NODEFILE")
            .ok()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .map(|content| {
                content
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let num_nodes = if node_list.is_empty() {
            None
        } else {
            Some(node_list.iter().collect::<std::collections::HashSet<_>>().len() as u32)
        };

        let num_tasks = env::var("PBS_NP").ok().and_then(|s| s.parse().ok());

        // Array job info
        let array_info = env::var("PBS_ARRAY_ID").ok().map(|array_job_id| {
            ArrayInfo {
                array_job_id,
                task_id: env::var("PBS_ARRAY_INDEX")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                task_count: None,
            }
        });

        Self {
            scheduler: SchedulerType::Pbs,
            job_id,
            job_name,
            num_nodes,
            num_tasks,
            cpus_per_task: None,
            memory: None,
            time_limit: None,
            work_dir,
            node_list,
            array_info,
        }
    }

    fn collect_lsf() -> Self {
        let job_id = env::var("LSB_JOBID").ok();
        let job_name = env::var("LSB_JOBNAME").ok();
        let num_tasks = env::var("LSB_DJOB_NUMPROC").ok().and_then(|s| s.parse().ok());

        let node_list: Vec<String> = env::var("LSB_HOSTS")
            .ok()
            .map(|s| s.split_whitespace().map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let num_nodes = if node_list.is_empty() {
            None
        } else {
            Some(node_list.iter().collect::<std::collections::HashSet<_>>().len() as u32)
        };

        let array_info = env::var("LSB_JOBINDEX").ok().and_then(|idx| {
            idx.parse().ok().map(|task_id| ArrayInfo {
                array_job_id: env::var("LSB_JOBID").unwrap_or_default(),
                task_id,
                task_count: None,
            })
        });

        Self {
            scheduler: SchedulerType::Lsf,
            job_id,
            job_name,
            num_nodes,
            num_tasks,
            cpus_per_task: None,
            memory: None,
            time_limit: None,
            work_dir: None,
            node_list,
            array_info,
        }
    }

    fn collect_sge() -> Self {
        let job_id = env::var("JOB_ID").ok();
        let job_name = env::var("JOB_NAME").ok();
        let num_tasks = env::var("NSLOTS").ok().and_then(|s| s.parse().ok());

        let node_list = env::var("PE_HOSTFILE")
            .ok()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .map(|content| {
                content
                    .lines()
                    .filter_map(|line| line.split_whitespace().next())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let array_info = env::var("SGE_TASK_ID").ok().and_then(|idx| {
            if idx == "undefined" {
                None
            } else {
                idx.parse().ok().map(|task_id| ArrayInfo {
                    array_job_id: env::var("JOB_ID").unwrap_or_default(),
                    task_id,
                    task_count: env::var("SGE_TASK_LAST").ok().and_then(|s| s.parse().ok()),
                })
            }
        });

        Self {
            scheduler: SchedulerType::Sge,
            job_id,
            job_name,
            num_nodes: None,
            num_tasks,
            cpus_per_task: None,
            memory: None,
            time_limit: None,
            work_dir: env::var("SGE_O_WORKDIR").ok().map(PathBuf::from),
            node_list,
            array_info,
        }
    }

    /// Get total available CPUs
    pub fn total_cpus(&self) -> Option<u32> {
        match (self.num_tasks, self.cpus_per_task) {
            (Some(tasks), Some(cpus)) => Some(tasks * cpus),
            (Some(tasks), None) => Some(tasks),
            (None, Some(cpus)) => Some(cpus),
            (None, None) => None,
        }
    }

    /// Check if running inside a job
    pub fn is_job(&self) -> bool {
        self.job_id.is_some()
    }
}

/// Job submission configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// Job name
    pub name: String,
    /// Number of nodes
    pub nodes: u32,
    /// Tasks per node
    pub tasks_per_node: u32,
    /// CPUs per task
    pub cpus_per_task: u32,
    /// Memory per node (bytes)
    pub memory: u64,
    /// Time limit (seconds)
    pub time_limit: u64,
    /// Partition/queue
    pub partition: Option<String>,
    /// Account/project
    pub account: Option<String>,
    /// Output file path
    pub output: Option<PathBuf>,
    /// Error file path
    pub error: Option<PathBuf>,
    /// Working directory
    pub work_dir: Option<PathBuf>,
    /// Additional environment variables
    pub environment: HashMap<String, String>,
    /// Array job range (start, end, step)
    pub array: Option<(u32, u32, u32)>,
    /// Dependencies (job IDs)
    pub dependencies: Vec<String>,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            name: "smartcopy".to_string(),
            nodes: 1,
            tasks_per_node: 1,
            cpus_per_task: 1,
            memory: 4 * 1024 * 1024 * 1024, // 4GB
            time_limit: 3600,               // 1 hour
            partition: None,
            account: None,
            output: None,
            error: None,
            work_dir: None,
            environment: HashMap::new(),
            array: None,
            dependencies: Vec::new(),
        }
    }
}

/// Job scheduler interface
pub struct JobScheduler {
    scheduler: SchedulerType,
}

impl JobScheduler {
    /// Create a new job scheduler interface
    pub fn new() -> Self {
        Self {
            scheduler: SchedulerType::detect(),
        }
    }

    /// Create for a specific scheduler type
    pub fn with_type(scheduler: SchedulerType) -> Self {
        Self { scheduler }
    }

    /// Get scheduler type
    pub fn scheduler_type(&self) -> SchedulerType {
        self.scheduler
    }

    /// Submit a SmartCopy job
    pub fn submit(&self, config: &JobConfig, smartcopy_args: &[&str]) -> io::Result<String> {
        match self.scheduler {
            SchedulerType::Slurm => self.submit_slurm(config, smartcopy_args),
            SchedulerType::Pbs | SchedulerType::PbsPro => self.submit_pbs(config, smartcopy_args),
            SchedulerType::Lsf => self.submit_lsf(config, smartcopy_args),
            SchedulerType::Sge => self.submit_sge(config, smartcopy_args),
            SchedulerType::None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No job scheduler detected",
            )),
        }
    }

    fn submit_slurm(&self, config: &JobConfig, smartcopy_args: &[&str]) -> io::Result<String> {
        let script = self.generate_slurm_script(config, smartcopy_args);

        let output = Command::new("sbatch")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?
            .wait_with_output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Parse "Submitted batch job 12345"
            Ok(stdout
                .split_whitespace()
                .last()
                .unwrap_or("unknown")
                .to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn generate_slurm_script(&self, config: &JobConfig, smartcopy_args: &[&str]) -> String {
        let mut script = String::from("#!/bin/bash\n");

        script.push_str(&format!("#SBATCH --job-name={}\n", config.name));
        script.push_str(&format!("#SBATCH --nodes={}\n", config.nodes));
        script.push_str(&format!("#SBATCH --ntasks-per-node={}\n", config.tasks_per_node));
        script.push_str(&format!("#SBATCH --cpus-per-task={}\n", config.cpus_per_task));
        script.push_str(&format!("#SBATCH --mem={}M\n", config.memory / (1024 * 1024)));
        script.push_str(&format!("#SBATCH --time={}\n", format_slurm_time(config.time_limit)));

        if let Some(ref partition) = config.partition {
            script.push_str(&format!("#SBATCH --partition={}\n", partition));
        }
        if let Some(ref account) = config.account {
            script.push_str(&format!("#SBATCH --account={}\n", account));
        }
        if let Some(ref output) = config.output {
            script.push_str(&format!("#SBATCH --output={}\n", output.display()));
        }
        if let Some(ref error) = config.error {
            script.push_str(&format!("#SBATCH --error={}\n", error.display()));
        }
        if let Some((start, end, step)) = config.array {
            script.push_str(&format!("#SBATCH --array={}-{}:{}\n", start, end, step));
        }
        if !config.dependencies.is_empty() {
            script.push_str(&format!(
                "#SBATCH --dependency=afterok:{}\n",
                config.dependencies.join(":")
            ));
        }

        script.push_str("\n# Environment\n");
        for (key, value) in &config.environment {
            script.push_str(&format!("export {}=\"{}\"\n", key, value));
        }

        script.push_str("\n# Run SmartCopy\n");
        script.push_str(&format!("smartcopy {}\n", smartcopy_args.join(" ")));

        script
    }

    fn submit_pbs(&self, config: &JobConfig, smartcopy_args: &[&str]) -> io::Result<String> {
        let script = self.generate_pbs_script(config, smartcopy_args);

        let output = Command::new("qsub")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?
            .wait_with_output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn generate_pbs_script(&self, config: &JobConfig, smartcopy_args: &[&str]) -> String {
        let mut script = String::from("#!/bin/bash\n");

        script.push_str(&format!("#PBS -N {}\n", config.name));
        script.push_str(&format!(
            "#PBS -l nodes={}:ppn={}\n",
            config.nodes,
            config.tasks_per_node * config.cpus_per_task
        ));
        script.push_str(&format!("#PBS -l mem={}mb\n", config.memory / (1024 * 1024)));
        script.push_str(&format!(
            "#PBS -l walltime={}\n",
            format_pbs_time(config.time_limit)
        ));

        if let Some(ref partition) = config.partition {
            script.push_str(&format!("#PBS -q {}\n", partition));
        }
        if let Some(ref account) = config.account {
            script.push_str(&format!("#PBS -A {}\n", account));
        }
        if let Some(ref output) = config.output {
            script.push_str(&format!("#PBS -o {}\n", output.display()));
        }
        if let Some(ref error) = config.error {
            script.push_str(&format!("#PBS -e {}\n", error.display()));
        }
        if let Some((start, end, _step)) = config.array {
            script.push_str(&format!("#PBS -t {}-{}\n", start, end));
        }

        script.push_str("\ncd $PBS_O_WORKDIR\n");
        script.push_str(&format!("smartcopy {}\n", smartcopy_args.join(" ")));

        script
    }

    fn submit_lsf(&self, config: &JobConfig, smartcopy_args: &[&str]) -> io::Result<String> {
        let mut args = vec![
            "-J".to_string(),
            config.name.clone(),
            "-n".to_string(),
            (config.nodes * config.tasks_per_node).to_string(),
            "-M".to_string(),
            format!("{}M", config.memory / (1024 * 1024)),
            "-W".to_string(),
            format!("{}", config.time_limit / 60),
        ];

        if let Some(ref queue) = config.partition {
            args.push("-q".to_string());
            args.push(queue.clone());
        }

        args.push("smartcopy".to_string());
        args.extend(smartcopy_args.iter().map(|s| s.to_string()));

        let output = Command::new("bsub")
            .args(&args)
            .output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Parse "Job <12345> is submitted..."
            Ok(stdout
                .split('<')
                .nth(1)
                .and_then(|s| s.split('>').next())
                .unwrap_or("unknown")
                .to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn submit_sge(&self, config: &JobConfig, smartcopy_args: &[&str]) -> io::Result<String> {
        let script = self.generate_sge_script(config, smartcopy_args);

        let output = Command::new("qsub")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?
            .wait_with_output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Parse "Your job 12345 ("name") has been submitted"
            Ok(stdout
                .split_whitespace()
                .nth(2)
                .unwrap_or("unknown")
                .to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    fn generate_sge_script(&self, config: &JobConfig, smartcopy_args: &[&str]) -> String {
        let mut script = String::from("#!/bin/bash\n");

        script.push_str(&format!("#$ -N {}\n", config.name));
        script.push_str(&format!(
            "#$ -pe smp {}\n",
            config.tasks_per_node * config.cpus_per_task
        ));
        script.push_str(&format!("#$ -l h_vmem={}M\n", config.memory / (1024 * 1024)));
        script.push_str(&format!(
            "#$ -l h_rt={}\n",
            format_pbs_time(config.time_limit)
        ));
        script.push_str("#$ -cwd\n");

        if let Some(ref queue) = config.partition {
            script.push_str(&format!("#$ -q {}\n", queue));
        }
        if let Some(ref output) = config.output {
            script.push_str(&format!("#$ -o {}\n", output.display()));
        }
        if let Some(ref error) = config.error {
            script.push_str(&format!("#$ -e {}\n", error.display()));
        }
        if let Some((start, end, step)) = config.array {
            script.push_str(&format!("#$ -t {}-{}:{}\n", start, end, step));
        }

        script.push_str(&format!("\nsmartcopy {}\n", smartcopy_args.join(" ")));

        script
    }

    /// Cancel a job
    pub fn cancel(&self, job_id: &str) -> io::Result<()> {
        let cmd = match self.scheduler {
            SchedulerType::Slurm => "scancel",
            SchedulerType::Pbs | SchedulerType::PbsPro => "qdel",
            SchedulerType::Lsf => "bkill",
            SchedulerType::Sge => "qdel",
            SchedulerType::None => return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No job scheduler detected",
            )),
        };

        let output = Command::new(cmd).arg(job_id).output()?;

        if output.status.success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    /// Get job status
    pub fn status(&self, job_id: &str) -> io::Result<JobStatus> {
        match self.scheduler {
            SchedulerType::Slurm => self.status_slurm(job_id),
            SchedulerType::Pbs | SchedulerType::PbsPro => self.status_pbs(job_id),
            SchedulerType::Lsf => self.status_lsf(job_id),
            SchedulerType::Sge => self.status_sge(job_id),
            SchedulerType::None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No job scheduler detected",
            )),
        }
    }

    fn status_slurm(&self, job_id: &str) -> io::Result<JobStatus> {
        let output = Command::new("squeue")
            .args(&["-j", job_id, "-h", "-o", "%T"])
            .output()?;

        if output.status.success() {
            let status = String::from_utf8_lossy(&output.stdout).trim().to_uppercase();
            Ok(match status.as_str() {
                "PENDING" | "CONFIGURING" => JobStatus::Pending,
                "RUNNING" | "COMPLETING" => JobStatus::Running,
                "COMPLETED" => JobStatus::Completed,
                "FAILED" | "TIMEOUT" | "NODE_FAIL" => JobStatus::Failed,
                "CANCELLED" => JobStatus::Cancelled,
                _ => JobStatus::Unknown,
            })
        } else {
            // Job might have completed, check sacct
            let output = Command::new("sacct")
                .args(&["-j", job_id, "-n", "-o", "State"])
                .output()?;

            if output.status.success() {
                let status = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_uppercase();

                Ok(match status.as_str() {
                    "COMPLETED" => JobStatus::Completed,
                    "FAILED" | "TIMEOUT" => JobStatus::Failed,
                    "CANCELLED" => JobStatus::Cancelled,
                    _ => JobStatus::Unknown,
                })
            } else {
                Ok(JobStatus::Unknown)
            }
        }
    }

    fn status_pbs(&self, job_id: &str) -> io::Result<JobStatus> {
        let output = Command::new("qstat").arg(job_id).output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Parse qstat output for job state
            for line in stdout.lines().skip(2) {
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() >= 5 {
                    return Ok(match fields[4] {
                        "Q" | "W" => JobStatus::Pending,
                        "R" | "E" => JobStatus::Running,
                        "C" => JobStatus::Completed,
                        "H" => JobStatus::Pending,
                        _ => JobStatus::Unknown,
                    });
                }
            }
        }

        Ok(JobStatus::Unknown)
    }

    fn status_lsf(&self, job_id: &str) -> io::Result<JobStatus> {
        let output = Command::new("bjobs")
            .args(&["-o", "stat", "-noheader", job_id])
            .output()?;

        if output.status.success() {
            let status = String::from_utf8_lossy(&output.stdout).trim().to_uppercase();
            Ok(match status.as_str() {
                "PEND" => JobStatus::Pending,
                "RUN" => JobStatus::Running,
                "DONE" => JobStatus::Completed,
                "EXIT" => JobStatus::Failed,
                _ => JobStatus::Unknown,
            })
        } else {
            Ok(JobStatus::Unknown)
        }
    }

    fn status_sge(&self, job_id: &str) -> io::Result<JobStatus> {
        let output = Command::new("qstat").args(&["-j", job_id]).output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.contains("job_state") {
                if stdout.contains("r") {
                    return Ok(JobStatus::Running);
                } else if stdout.contains("qw") {
                    return Ok(JobStatus::Pending);
                }
            }
        }

        // Job not found, might be completed
        Ok(JobStatus::Completed)
    }
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Job status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

// Helper functions

fn which(cmd: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths).find_map(|dir| {
            let full_path = dir.join(cmd);
            if full_path.is_file() {
                Some(full_path)
            } else {
                None
            }
        })
    })
}

fn run_command(cmd: &str, args: &[&str]) -> Option<Output> {
    Command::new(cmd).args(args).output().ok()
}

fn parse_memory(s: String) -> Option<u64> {
    let s = s.trim().to_uppercase();
    let (num_str, mult) = if s.ends_with("G") {
        (&s[..s.len() - 1], 1024 * 1024 * 1024u64)
    } else if s.ends_with("M") {
        (&s[..s.len() - 1], 1024 * 1024u64)
    } else if s.ends_with("K") {
        (&s[..s.len() - 1], 1024u64)
    } else {
        (s.as_str(), 1u64)
    };

    num_str.parse::<u64>().ok().map(|n| n * mult)
}

fn parse_slurm_time(s: String) -> Option<u64> {
    // Format: D-HH:MM:SS or HH:MM:SS
    let parts: Vec<&str> = s.split('-').collect();
    let (days, time_str) = if parts.len() == 2 {
        (parts[0].parse::<u64>().unwrap_or(0), parts[1])
    } else {
        (0, parts[0])
    };

    let time_parts: Vec<&str> = time_str.split(':').collect();
    let (hours, minutes, seconds) = match time_parts.len() {
        3 => (
            time_parts[0].parse::<u64>().unwrap_or(0),
            time_parts[1].parse::<u64>().unwrap_or(0),
            time_parts[2].parse::<u64>().unwrap_or(0),
        ),
        2 => (
            time_parts[0].parse::<u64>().unwrap_or(0),
            time_parts[1].parse::<u64>().unwrap_or(0),
            0,
        ),
        _ => return None,
    };

    Some(days * 86400 + hours * 3600 + minutes * 60 + seconds)
}

fn format_slurm_time(seconds: u64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}-{:02}:{:02}:{:02}", days, hours, minutes, secs)
    } else {
        format!("{:02}:{:02}:{:02}", hours, minutes, secs)
    }
}

fn format_pbs_time(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, secs)
}

fn expand_node_list(nodelist: &str) -> Vec<String> {
    // Simple expansion for formats like "node[01-04]" or "node1,node2,node3"
    let mut nodes = Vec::new();

    if nodelist.contains('[') {
        // Range format: node[01-04]
        if let Some(bracket_pos) = nodelist.find('[') {
            let prefix = &nodelist[..bracket_pos];
            if let Some(end_pos) = nodelist.find(']') {
                let range = &nodelist[bracket_pos + 1..end_pos];
                if let Some(dash_pos) = range.find('-') {
                    let start = &range[..dash_pos];
                    let end = &range[dash_pos + 1..];
                    if let (Ok(s), Ok(e)) = (start.parse::<u32>(), end.parse::<u32>()) {
                        let width = start.len();
                        for i in s..=e {
                            nodes.push(format!("{}{:0width$}", prefix, i, width = width));
                        }
                    }
                }
            }
        }
    } else {
        // Comma-separated format
        nodes = nodelist.split(',').map(|s| s.trim().to_string()).collect();
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_node_list() {
        let nodes = expand_node_list("node[01-04]");
        assert_eq!(nodes, vec!["node01", "node02", "node03", "node04"]);

        let nodes = expand_node_list("node1,node2,node3");
        assert_eq!(nodes, vec!["node1", "node2", "node3"]);
    }

    #[test]
    fn test_format_slurm_time() {
        assert_eq!(format_slurm_time(3600), "01:00:00");
        assert_eq!(format_slurm_time(90061), "1-01:01:01");
    }

    #[test]
    fn test_parse_memory() {
        assert_eq!(parse_memory("4G".to_string()), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_memory("512M".to_string()), Some(512 * 1024 * 1024));
        assert_eq!(parse_memory("1024K".to_string()), Some(1024 * 1024));
    }
}
