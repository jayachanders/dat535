# GitHub Actions CI/CD Guide

This folder contains the automated workflows for the DAT535 project. These workflows use GitHub Actions to schedule data pipelines, run tests, and deploy code.

**Important**: These pipelines are designed to execute on **self-hosted runners**.

## Available Workflows

We have configured the following automated processes:

### 1. Daily Pipeline (`daily_pipeline.yml`)

- **Purpose**: Runs heavy data processing tasks once a day.
- **Trigger**: Scheduled cron job (usually midnight UTC) or manual dispatch.
- **Key Tasks**:
  - Checks out code.
  - Sets up Python environment.
  - Executes `run_pipeline.py --mode daily`.

### 2. Hourly Pipeline (`hourly_pipeline.yml`)

- **Purpose**: Runs incremental data updates or quick checks.
- **Trigger**: Scheduled cron job (every hour) or manual dispatch.
- **Key Tasks**:
  - Executes `run_pipeline.py --mode hourly`.

### 3. Deploy and Run (`deploy-and-run.yml`)

- **Purpose**: Validates code changes and simulates a deployment.
- **Trigger**: Pushes to the `main` branch.
- **Key Tasks**:
  - Runs unit tests (if configured).
  - Verifies dependencies install correctly.

## Infrastructure & Runners

These workflows use `runs-on: self-hosted` to target our OpenStack infrastructure. This provides:

- Access to private data sources.
- Consistent hardware resources defined by the course instructors.

**⚠️ Note for Personal Forks:**
If you fork this repository to your personal GitHub account, you will not have access to the self-hosted runners. You must setup your runner with Spark installed.

## Setting up a Self-Hosted Runner

To execute these pipelines on your own infrastructure (e.g., your own VM), follow these steps to install the runner:

### Step 1: Enable Actions

1. Go to your repository on GitHub.
2. Click the **Settings** tab.
3. Go to actions -> Runners -> Select **New self-hosted runner**
4. Then select the runner where your spark cluster is running for example Linux.
5. Select x64 architecture, it will show set commands to run on the self-hosted runner.
6. Run those commands on VM.

```bash
# Create a folder
mkdir actions-runner && cd actions-runner

# Download the runner package (Get the specific link for your OS from GitHub UI > Settings > Actions > Runners)
curl -o actions-runner-osx-x64-2.331.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.331.0/actions-runner-osx-x64-2.331.0.tar.gz

# Extract
tar xzf ./actions-runner-osx-x64-2.331.0.tar.gz

# Configure (You will need the token from the GitHub UI)
./config.sh --url https://github.com/OWNER/REPO --token YOUR_TOKEN

# Start the runner
# To keep the runner alive after closing the terminal, install as a service:
sudo ./svc.sh install 
sudo ./svc.sh start
```

### Step 2: Triggering Manually

You can test a pipeline without waiting for the schedule:

1. Go to the **Actions** tab.
2. Select a workflow (e.g., "Daily Pipeline") from the left sidebar.
3. Click the **Run workflow** dropdown button on the right.
4. Click **Run workflow**.

## Troubleshooting

- **Python Version Mismatch**: Ensure the `python-version` in the YAML file matches your local development environment.
- **Missing Dependencies**: If a job fails during installation, check that all packages are listed in your requirements.txt or setup.py.
- **Permission Denied**: If a script fails to run, you may need to make it executable: git update-index --chmod=+x run_pipeline.py.
