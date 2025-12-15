# GitHub Repository Setup Guide - Core CI/CD Deployment

This guide explains how to set up your GitHub repository for automated CI/CD deployment of the Wistia Analytics Pipeline.

## **FOLDER STRUCTURE TO CREATE**

```
wistia-analytics-pipeline/
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yml          ← Lambda deployment
│       ├── deploy-glue.yml            ← Glue job deployment
│       ├── deploy-streamlit.yml       ← Streamlit dashboard deployment
│       └── setup-athena.yml           ← Athena database setup
│
├── lambda/
│   ├── src/
│   │   └── index.py                   ← Lambda ingestion code
│   └── requirements.txt                ← Lambda dependencies
│
├── glue/
│   └── src/
│       └── transform_wistia_curated.py ← Glue ETL code
│
├── streamlit/
│   ├── app.py                         ← Dashboard code
│   ├── requirements.txt                ← Streamlit dependencies
│   └── .streamlit/
│       ├── config.toml                 ← Streamlit configuration
│       └── secrets.toml                ← Streamlit secrets
│
├── config/
│   └── athena-ddl.sql                 ← Athena table definitions
│
├── .gitignore
└── README.md

```

---

## **STEP 1: Create GitHub Repository**

### 1.1 Create new repository
```bash
# Create local repository
mkdir wistia-analytics-pipeline
cd wistia-analytics-pipeline
git init
git config user.name "Your Name"
git config user.email "your.email@example.com"
```

### 1.2 Create folder structure
```bash
mkdir -p .github/workflows
mkdir -p lambda/src
mkdir -p glue/src
mkdir -p streamlit/.streamlit
mkdir -p config
```

### 1.3 Push to GitHub
```bash
# Create repo on GitHub.com first, then:
git remote add origin https://github.com/<your-username>/wistia-analytics-pipeline.git
git branch -M main
```

---

## **STEP 2: Add GitHub Actions Workflows**

Copy the following files to `.github/workflows/`:

### 2.1 Lambda deployment workflow
**File:** `.github/workflows/deploy-lambda.yml`
- Builds Lambda deployment package
- Uploads to S3
- Updates Lambda function
- Runs test invocation

### 2.2 Glue deployment workflow
**File:** `.github/workflows/deploy-glue.yml`
- Uploads Glue script to S3
- Verifies job configuration

### 2.3 Streamlit deployment workflow
**File:** `.github/workflows/deploy-streamlit.yml`
- Validates Python code
- Checks dependencies
- Auto-deploys to Streamlit Cloud

### 2.4 Athena setup workflow
**File:** `.github/workflows/setup-athena.yml`
- Creates Athena database
- Creates tables from SQL
- Verifies table creation

---

## **STEP 3: Add Application Code**

### 3.1 Lambda function
**File:** `lambda/src/index.py`
```bash
# Copy your existing Lambda code here
cp ../wistia_ingestion_lambda.py lambda/src/index.py
```

### 3.2 Lambda requirements
**File:** `lambda/requirements.txt`
```
boto3
requests
```

### 3.3 Glue job
**File:** `glue/src/transform_wistia_curated.py`
```bash
# Copy your existing Glue code here
cp ../glue_etl_transform_FIXED.py glue/src/transform_wistia_curated.py
```

### 3.4 Streamlit dashboard
**File:** `streamlit/app.py`
```bash
# Copy your existing Streamlit app here
cp ../streamlit_dashboard_app.py streamlit/app.py
```

### 3.5 Streamlit requirements
**File:** `streamlit/requirements.txt`
```
streamlit==1.28.1
pyathena==2.25.2
pandas==2.1.3
plotly==5.18.0
boto3==1.29.7
```

### 3.6 Streamlit config
**File:** `streamlit/.streamlit/config.toml`
```toml
[client]
showErrorDetails = true

[logger]
level = "info"

[server]
maxUploadSize = 200
```

### 3.7 Athena DDL
**File:** `config/athena-ddl.sql`
```bash
# Copy your existing Athena DDL here
cp ../athena_ddl_create_tables.sql config/athena-ddl.sql
```

---

## **STEP 4: Add Gitignore**

**File:** `.gitignore`
```
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# AWS
.aws/
*.pem

# Streamlit
.streamlit/secrets.toml
.streamlit/cache/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local
venv/
env/
```

---

## **STEP 5: Add README**

**File:** `README.md`
```markdown
# Wistia Analytics Pipeline

Automated data pipeline with Lambda, Glue, Athena, and Streamlit dashboard.

## Architecture

- **Ingestion:** AWS Lambda (daily at 2 AM UTC)
- **Transformation:** AWS Glue (Parquet format)
- **Analytics:** Amazon Athena (SQL queries)
- **Dashboard:** Streamlit (interactive visualizations)

## Quick Start

### Local Setup
```bash
pip install -r streamlit/requirements.txt
streamlit run streamlit/app.py
```

### Deploy
Push to main branch - GitHub Actions handles deployment!

## GitHub Secrets Required

Configure these in GitHub Settings → Secrets:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION
- WISTIA_API_TOKEN
- STREAMLIT_AUTH_TOKEN

## Files Structure

- `lambda/` - Lambda ingestion code
- `glue/` - Glue ETL job
- `streamlit/` - Dashboard application
- `config/` - Configuration files
- `.github/workflows/` - CI/CD pipelines
```

---

## **STEP 6: Configure GitHub Secrets**

### 6.1 Get your secrets
```bash
# AWS credentials
aws sts get-caller-identity

# Get Lambda role ARN
aws iam get-role --role-name wistia-lambda-execution-role --query 'Role.Arn'

# Get Glue role ARN
aws iam get-role --role-name wistia-glue-execution-role --query 'Role.Arn'
```

### 6.2 Add secrets to GitHub
1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline/settings/secrets/actions
2. Click "New repository secret"
3. Add these secrets:

```
AWS_ACCESS_KEY_ID = <your-access-key>
AWS_SECRET_ACCESS_KEY = <your-secret-key>
AWS_REGION = us-east-1
WISTIA_API_TOKEN = <your-wistia-token>
STREAMLIT_AUTH_TOKEN = <your-streamlit-token>
```

---

## **STEP 7: Push to GitHub**

```bash
# Stage all files
git add .

# Commit
git commit -m "Initial Wistia analytics pipeline setup"

# Push to GitHub
git push -u origin main
```

---

## **STEP 8: Verify Workflows Running**

1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline/actions
2. You should see workflows running:
   - ✅ Deploy Lambda Function
   - ✅ Deploy Glue ETL Job
   - ✅ Deploy Streamlit Dashboard
   - ✅ Setup Athena Database

---

## **CI/CD FLOW**

### When you push to main:

```
Push code to GitHub
    ↓
GitHub Actions triggers workflows
    ├─ Lambda: builds, packages, uploads, deploys
    ├─ Glue: uploads script to S3
    ├─ Streamlit: validates, deploys to Streamlit Cloud
    └─ Athena: creates/updates database tables
    ↓
Workflows complete (check status in Actions tab)
    ↓
✅ All services updated!
```

---

## **MODIFYING CODE**

### To update Lambda function:
```bash
# Edit lambda/src/index.py
# Commit and push
git add lambda/src/index.py
git commit -m "Update Lambda ingestion logic"
git push

# Workflow automatically:
# 1. Packages code
# 2. Uploads to S3
# 3. Updates Lambda function
# 4. Tests with invoke
```

### To update Glue job:
```bash
# Edit glue/src/transform_wistia_curated.py
# Commit and push
git add glue/src/transform_wistia_curated.py
git commit -m "Update Glue transformation logic"
git push

# Workflow automatically:
# 1. Uploads to S3
# 2. Verifies job configuration
```

### To update Streamlit dashboard:
```bash
# Edit streamlit/app.py
# Commit and push
git add streamlit/app.py
git commit -m "Add new dashboard chart"
git push

# Workflow automatically:
# 1. Validates code
# 2. Checks dependencies
# 3. Deploys to Streamlit Cloud
```

### To update Athena tables:
```bash
# Edit config/athena-ddl.sql
# Commit and push
git add config/athena-ddl.sql
git commit -m "Add new table schema"
git push

# Workflow automatically:
# 1. Executes SQL
# 2. Creates/updates tables
# 3. Verifies table creation
```

---

## **MONITORING DEPLOYMENTS**

### View workflow status
1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline/actions
2. Click on any workflow run
3. See detailed logs for each step

### Example logs you'll see:
```
✅ Checkout code
✅ Set up Python 3.11
✅ Install dependencies
✅ Create deployment package
✅ Configure AWS credentials
✅ Upload to S3
✅ Update Lambda function
✅ Test Lambda function
✅ Check Lambda logs
✅ Notify on success
```

---

## **TROUBLESHOOTING**

### Workflow fails with "Permission denied"
**Solution:** Check GitHub Secrets are set correctly
```bash
# Verify credentials work locally
aws sts get-caller-identity
```

### Lambda deployment fails
**Solution:** Check S3 bucket exists
```bash
aws s3 ls s3://wistia-analytics/
```

### Streamlit deployment fails
**Solution:** Check Streamlit Cloud is connected
- Go to https://share.streamlit.io
- Authorize GitHub repository
- Set STREAMLIT_AUTH_TOKEN secret

### Athena workflow fails
**Solution:** Verify Athena results bucket
```bash
aws s3 ls wistia-analytics-athena-results/
```

---

## **NEXT STEPS**

Once CI/CD is working:

1. **Make changes locally**
   ```bash
   git checkout -b feature/add-new-metric
   # Edit code
   git push origin feature/add-new-metric
   ```

2. **Create Pull Request**
   - GitHub will run checks
   - Review and merge to main
   - Workflows automatically deploy

3. **Monitor deployments**
   - Check Actions tab for status
   - View logs for any issues
   - Rollback by reverting commit if needed

---

## **KEY FEATURES OF THIS SETUP**

✅ **Automatic deployments** - Push → Deploy
✅ **Code validation** - Linting, syntax checks
✅ **Integration testing** - Lambda invoke test
✅ **Configuration management** - All in Git
✅ **Easy rollback** - Just revert last commit
✅ **Secrets secure** - AWS credentials encrypted
✅ **Audit trail** - All deployments logged
✅ **Team friendly** - Pull request workflow

---

## **SUPPORT**

- **GitHub Actions docs:** https://docs.github.com/en/actions
- **AWS CLI reference:** https://docs.aws.amazon.com/cli/
- **Streamlit docs:** https://docs.streamlit.io
- **Athena docs:** https://docs.aws.amazon.com/athena/

---

**Your CI/CD pipeline is ready to go!** 🚀
