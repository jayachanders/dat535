# Lab 1.2 — CI/CD Pipelines: Dev/Prod with GitHub Actions

## Learning Objectives

- Understand Dev/Prod environment separation
- Configure GitHub Environments with approval gates
- Work with the dev/main branch strategy from VSCode

---

## The Dev/Prod Model

1. Work on `dev` branch locally or on the cluster
2. Push to `dev` — the `.github/workflows/dat535-dev.yml` pipeline runs automatically
3. Open a Pull Request from `dev` to `main` to release
4. After merge, `.github/workflows/dat535-prod.yml` runs and requires approval for deployment

## Part A: Configure GitHub Environments

### 1. Create the `dev` environment

1. Go to your GitHub repo → **Settings** → **Environments** → **New environment**
2. Name it `dev` → **Configure environment**
3. Leave "Protection rules" empty (dev auto-deploys on every push)

### 2. Create the `prod` environment (main)

1. **New environment** → name it the prod environment `main`
2. Under **Deployment protection rules** → enable **Required reviewers**
3. Add yourself and / or group partner as a reviewer
4. Save — now every prod deploy requires a manual approval click

---

## Part B: Branch Strategy

```bash
# Day-to-day development (dev branch) 
git checkout -b dev
# work in VSCode
git add .
# if you have made changes in lab2
git commit -m "feat(lab2): implement ingest"
git push origin dev
```

1. GitHub Actions dev pipeline starts automatically
2. Watch it at: github.com/YourGitHubUsername/groupXX-dat535

### Release to production

1. Open a Pull Request on GitHub: dev → main
2. Reviewer reviews the diff and approves
3. Merge the PR
    → GitHub Actions prod pipeline starts
    → Reviewer is asked to approve the deployment
    → After approval, prod pipeline runs

Open a PR `dev` → `main`, request review, merge when ready.

---

## Viewing Results

### GitHub Actions tab in the browser

github.com/YourGitHubUsername/groupXX-dat535 → Actions tab → select a run → view live logs.

### GitHub Actions VSCode Extension

With the GitHub Actions extension installed:

Click the GitHub Actions icon in the Activity Bar
See all workflow runs and their status
Click a run to view logs inline in VSCode

---

## How the Pipeline Code is Environment-Aware

The pipeline scripts read the environment from:

--env argument: python run_pipeline.py lab2 --env prod
Environment variable: DAT535_ENV=prod python run_pipeline.py lab2

```bash
class Lab2Pipeline:
    def __init__(self, env: str = "dev"):
        self.env = env
        # dev writes to ~/spark-lab-data/dev/lab2/
        # prod writes to ~/spark-lab-data/prod/lab2/
        self.base_dir = f"~/spark-lab-data/{env}/lab2"
```

---

## Hands-On Exercise

1. Create `dev` branch and push a trivial change to trigger the dev pipeline

    git checkout -b dev && git push -u origin dev

    Add a `print("Hello from dev pipeline")` line to `dat535/lab2/lab2_pipeline.py`

    Commit and push to dev — watch the dev pipeline run

2. Open a PR and merge to `main` to trigger the prod pipeline (approval required)

    Watch the prod pipeline — approve the deployment

---

## Checklist

- [ ] dev GitHub Environment created (no protection rules)
- [ ] prod GitHub Environment created (with required reviewer)
- [ ] Workflow files exist at .github/workflows/dat535-dev.yml and dat535-prod.yml
- [ ] Pushed to dev branch — pipeline succeeded
- [ ] Opened PR, merged to main — prod pipeline ran with approval
- [ ] Downloaded and opened artifacts in VSCode
