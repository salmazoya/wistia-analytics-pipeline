# Complete GitHub Push & CI/CD Deployment Guide

This guide walks you through setting up everything and pushing to GitHub with working CI/CD pipelines.

---

## **PHASE 1: LOCAL SETUP (30 minutes)**

### Step 1.1: Create local repository
```bash
# Create and navigate to project directory
mkdir -p ~/Projects/wistia-analytics-pipeline
cd ~/Projects/wistia-analytics-pipeline

# Initialize git
git init
git config user.name "Your Name"
git config user.email "your.email@example.com"
```

### Step 1.2: Create folder structure
```bash
# Create all necessary folders
mkdir -p .github/workflows
mkdir -p lambda/src
mkdir -p glue/src
mkdir -p streamlit/.streamlit
mkdir -p config

echo "✅ Folder structure created"
```

### Step 1.3: Add workflow files
Copy these files to `.github/workflows/`:
- `deploy-lambda.yml`
- `deploy-glue.yml`
- `deploy-streamlit.yml`
- `setup-athena.yml`

```bash
# Copy workflow files
cp ~/Downloads/deploy-lambda.yml .github/workflows/
cp ~/Downloads/deploy-glue.yml .github/workflows/
cp ~/Downloads/deploy-streamlit.yml .github/workflows/
cp ~/Downloads/setup-athena.yml .github/workflows/

ls -la .github/workflows/
```

---

## **PHASE 2: ADD APPLICATION CODE (20 minutes)**

### Step 2.1: Add Lambda code
```bash
# Copy Lambda ingestion code
cat > lambda/src/index.py << 'EOF'
"""
Wistia Analytics Lambda Function
(Paste your existing Lambda code here from wistia_ingestion_lambda.py)
"""
EOF

# Add Lambda dependencies
cat > lambda/requirements.txt << 'EOF'
boto3
requests
EOF

echo "✅ Lambda code added"
```

### Step 2.2: Add Glue code
```bash
# Copy Glue ETL code
cat > glue/src/transform_wistia_curated.py << 'EOF'
"""
Wistia Analytics Glue ETL Job
(Paste your existing Glue code here from glue_etl_transform_FIXED.py)
"""
EOF

echo "✅ Glue code added"
```

### Step 2.3: Add Streamlit code
```bash
# Copy Streamlit dashboard
cat > streamlit/app.py << 'EOF'
"""
Wistia Analytics Dashboard
(Paste your existing Streamlit code here)
"""
EOF

# Add Streamlit dependencies
cat > streamlit/requirements.txt << 'EOF'
streamlit==1.28.1
pyathena==2.25.2
pandas==2.1.3
plotly==5.18.0
boto3==1.29.7
EOF

# Add Streamlit config
cat > streamlit/.streamlit/config.toml << 'EOF'
[client]
showErrorDetails = true

[logger]
level = "info"

[server]
maxUploadSize = 200
EOF

echo "✅ Streamlit code added"
```

### Step 2.4: Add Athena DDL
```bash
# Copy Athena table definitions
cat > config/athena-ddl.sql << 'EOF'
-- Paste your existing Athena DDL here
-- From athena_ddl_create_tables.sql
EOF

echo "✅ Athena DDL added"
```

### Step 2.5: Add .gitignore
```bash
# Copy .gitignore file
cp ~/.gitignore .

echo "✅ .gitignore added"
```

### Step 2.6: Add README
```bash
# Copy README
cp ~/Downloads/README_GITHUB.md README.md

echo "✅ README.md added"
```

---

## **PHASE 3: CREATE GITHUB REPOSITORY (5 minutes)**

### Step 3.1: Create on GitHub
1. Go to: https://github.com/new
2. Repository name: `wistia-analytics-pipeline`
3. Description: `Automated Wistia video analytics pipeline`
4. Choose: **Public** (for Streamlit Cloud)
5. Click "Create repository"

### Step 3.2: Connect local to GitHub
```bash
# Add remote
git remote add origin https://github.com/<your-username>/wistia-analytics-pipeline.git

# Set main branch
git branch -M main

# Verify
git remote -v
# Should show:
# origin  https://github.com/<your-username>/wistia-analytics-pipeline.git (fetch)
# origin  https://github.com/<your-username>/wistia-analytics-pipeline.git (push)

echo "✅ GitHub remote added"
```

---

## **PHASE 4: STAGE AND COMMIT (5 minutes)**

### Step 4.1: Check what's ready
```bash
# See all files
git status

# Should show all new files
```

### Step 4.2: Stage all files
```bash
git add .

# Verify staging
git status
# All files should be staged (green)
```

### Step 4.3: Commit
```bash
git commit -m "Initial Wistia analytics pipeline with CI/CD

- GitHub Actions workflows for Lambda, Glue, Streamlit, Athena
- Lambda ingestion function
- Glue ETL transformation
- Streamlit interactive dashboard
- Athena database setup
- Complete CI/CD pipeline ready to deploy"

echo "✅ Files committed"
```

---

## **PHASE 5: PUSH TO GITHUB (2 minutes)**

### Step 5.1: Push code
```bash
# Push to GitHub
git push -u origin main

# You should see:
# Enumerating objects...
# Writing objects...
# Total X (delta Y), reused 0 (delta 0), pack-reused 0
# To https://github.com/<your-username>/wistia-analytics-pipeline.git
#  * [new branch]      main -> main

echo "✅ Code pushed to GitHub"
```

### Step 5.2: Verify on GitHub
1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline
2. You should see all files and folders
3. Check `.github/workflows/` has all 4 YAML files

---

## **PHASE 6: CONFIGURE GITHUB SECRETS (10 minutes)**

### Step 6.1: Get your secrets
```bash
# Get AWS credentials
aws sts get-caller-identity
# Note: Account ID (12 digits)

# Get access key and secret (from AWS IAM console or use existing)
echo "Access Key: <your-access-key>"
echo "Secret Key: <your-secret-key>"

# Get Lambda and Glue role ARNs
aws iam get-role --role-name wistia-lambda-execution-role --query 'Role.Arn' --output text

aws iam get-role --role-name wistia-glue-execution-role --query 'Role.Arn' --output text
```

### Step 6.2: Add secrets to GitHub
1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline/settings/secrets/actions
2. Click "New repository secret"
3. Add each secret:

**Secret 1: AWS_ACCESS_KEY_ID**
- Name: `AWS_ACCESS_KEY_ID`
- Value: `AKIAIOSFODNN7EXAMPLE` (your access key)

**Secret 2: AWS_SECRET_ACCESS_KEY**
- Name: `AWS_SECRET_ACCESS_KEY`
- Value: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` (your secret key)

**Secret 3: AWS_REGION**
- Name: `AWS_REGION`
- Value: `us-east-1`

**Secret 4: WISTIA_API_TOKEN** (optional for now)
- Name: `WISTIA_API_TOKEN`
- Value: `your-wistia-token`

**Secret 5: STREAMLIT_AUTH_TOKEN** (optional for now)
- Name: `STREAMLIT_AUTH_TOKEN`
- Value: `(from Streamlit Cloud settings)`

```bash
echo "✅ GitHub Secrets configured"
```

---

## **PHASE 7: TRIGGER WORKFLOWS (2 minutes)**

### Step 7.1: Check workflows
1. Go to: https://github.com/<your-username>/wistia-analytics-pipeline/actions
2. You should see:
   - Deploy Lambda Function
   - Deploy Glue ETL Job
   - Deploy Streamlit Dashboard
   - Setup Athena Database

### Step 7.2: Watch workflows run
Workflows trigger automatically when you push code that changes:
- `lambda/` → Deploy Lambda workflow
- `glue/` → Deploy Glue workflow
- `streamlit/` → Deploy Streamlit workflow
- `config/athena-ddl.sql` → Setup Athena workflow

1. Click on "Deploy Lambda Function" workflow
2. Watch the steps execute:
   - ✅ Checkout code
   - ✅ Set up Python 3.11
   - ✅ Install dependencies
   - ✅ Create deployment package
   - ✅ Configure AWS credentials
   - ✅ Upload to S3
   - ✅ Update Lambda function
   - ✅ Test Lambda function

---

## **PHASE 8: VERIFY DEPLOYMENTS (10 minutes)**

### Step 8.1: Check Lambda deployed
```bash
# Verify Lambda updated
aws lambda get-function-configuration \
  --function-name wistia-ingestion-lambda \
  --query '[FunctionName,LastModified,CodeSize]' \
  --output table

# Should show recent LastModified time
```

### Step 8.2: Check Glue script uploaded
```bash
# Verify Glue script in S3
aws s3 ls s3://wistia-analytics/glue-scripts/

# Should show transform_wistia_curated.py
```

### Step 8.3: Check Athena database
```bash
# Verify Athena database exists
aws athena start-query-execution \
  --query-string "SHOW TABLES IN wistia_analytics" \
  --query-execution-context Database=wistia_analytics \
  --result-configuration OutputLocation=s3://wistia-analytics-athena-results/

# Query ID returned = success
```

### Step 8.4: Check Streamlit deployment
1. Go to: https://share.streamlit.io/
2. Look for: `<your-username>/wistia-analytics-dashboard`
3. Should show as "Deployed"

---

## **PHASE 9: MAKE YOUR FIRST CHANGE (optional)**

Test the CI/CD pipeline by making a small change:

### Step 9.1: Make a change
```bash
# Edit Streamlit app
nano streamlit/app.py

# Change the title to test
# st.title("📊 Wistia Analytics Dashboard - Updated!")

# Save and exit
```

### Step 9.2: Commit and push
```bash
git add streamlit/app.py
git commit -m "Update dashboard title"
git push

echo "✅ Change pushed - watch workflows run!"
```

### Step 9.3: Watch deployment
1. Go to Actions tab
2. See "Deploy Streamlit Dashboard" workflow running
3. When complete, refresh Streamlit dashboard
4. See your change live!

---

## **CHECKLIST: IS EVERYTHING WORKING?**

✅ GitHub repository created
✅ All files pushed to GitHub
✅ GitHub Secrets configured
✅ `.github/workflows/*.yml` files present
✅ Deploy Lambda workflow completed
✅ Deploy Glue workflow completed
✅ Setup Athena workflow completed
✅ Lambda function updated in AWS
✅ Glue script uploaded to S3
✅ Athena database created
✅ Streamlit dashboard deployed

**If all checked:** Your CI/CD pipeline is working! 🎉

---

## **TROUBLESHOOTING**

### Workflows not running?
```bash
# Check if files were committed
git log --oneline
# Should show your commits

# Check if files in right location
git ls-files | grep workflows
# Should show .github/workflows/*.yml
```

### Deploy Lambda workflow failing?
```bash
# Check Lambda function exists
aws lambda get-function --function-name wistia-ingestion-lambda

# Check S3 bucket exists
aws s3 ls s3://wistia-analytics/
```

### Streamlit not deploying?
1. Go to https://share.streamlit.io → Settings
2. Connect GitHub repository
3. Set: `streamlit/app.py` as main file
4. Add `STREAMLIT_AUTH_TOKEN` to GitHub Secrets

### Athena setup failing?
```bash
# Check Athena results bucket
aws s3 ls wistia-analytics-athena-results/

# Check database exists
aws athena list-work-groups
```

---

## **NEXT STEPS**

Once CI/CD is working:

1. **Continue development**
   ```bash
   git checkout -b feature/add-new-metric
   # Make changes
   git push origin feature/add-new-metric
   # Create Pull Request on GitHub
   ```

2. **Monitor deployments**
   - Check Actions tab daily
   - Review logs for any issues
   - Fix and push new changes

3. **Add more features**
   - Update Lambda ingestion
   - Add Glue transformations
   - Enhance Streamlit dashboard
   - Extend Athena tables

4. **Scale up** (future phases)
   - Add unit tests
   - Add integration tests
   - Add Terraform IaC
   - Add Docker for local dev

---

## **SUMMARY**

You now have:
- ✅ **GitHub repository** with all code
- ✅ **CI/CD pipelines** for Lambda, Glue, Streamlit, Athena
- ✅ **Automated deployments** on every push
- ✅ **Live Streamlit dashboard**
- ✅ **Production-ready infrastructure**

**Everything is now automated!** 🚀

Every time you:
- Edit `lambda/src/index.py` → Lambda auto-deploys
- Edit `glue/src/transform_wistia_curated.py` → Glue auto-updates
- Edit `streamlit/app.py` → Dashboard auto-reloads
- Edit `config/athena-ddl.sql` → Tables auto-create

No more manual deployments needed! 🎉

---

**Questions?** Check the GitHub Actions logs in the Actions tab for detailed error messages.

**Ready?** Let's deploy! 🚀
