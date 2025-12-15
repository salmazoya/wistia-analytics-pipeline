# Wistia Analytics Pipeline 📊

Fully automated data pipeline for Wistia video analytics with GitHub Actions CI/CD, AWS Lambda, Glue, Athena, and Streamlit dashboard.

## Architecture

```
Wistia API
    ↓
Lambda (Daily ingestion) [Automated by EventBridge]
    ↓
S3 Raw Zone (JSON)
    ↓
Glue ETL (Transformation)
    ↓
S3 Curated Zone (Parquet)
    ↓
Athena Database (SQL queries)
    ↓
Streamlit Dashboard (Interactive visualizations)
```

## Features

✅ **Automated Deployments** - Push to main → Auto-deploy via GitHub Actions  
✅ **Infrastructure as Code** - All AWS resources configured in code  
✅ **Data Pipeline** - Lambda → Glue → Athena → Streamlit  
✅ **Interactive Dashboard** - Real-time Wistia analytics  
✅ **CI/CD Ready** - Test, validate, and deploy automatically  
✅ **Secure** - All secrets encrypted in GitHub  

## Quick Start

### Prerequisites
- GitHub account
- AWS account with credentials
- Python 3.11+
- Wistia API token

### 1. Clone Repository
```bash
git clone https://github.com/<your-username>/wistia-analytics-pipeline.git
cd wistia-analytics-pipeline
```

### 2. Configure GitHub Secrets
Go to Settings → Secrets → Actions and add:
```
AWS_ACCESS_KEY_ID        - Your AWS access key
AWS_SECRET_ACCESS_KEY    - Your AWS secret key
AWS_REGION              - us-east-1
WISTIA_API_TOKEN        - Your Wistia API token
STREAMLIT_AUTH_TOKEN    - Streamlit Cloud token
```

### 3. Push Code
```bash
git add .
git commit -m "Initial setup"
git push origin main
```

Workflows automatically deploy to AWS! ✨

### 4. Access Dashboard
Dashboard deployed to: `https://share.streamlit.io/<username>/wistia-analytics-dashboard`

## Folder Structure

```
wistia-analytics-pipeline/
├── .github/workflows/          # GitHub Actions CI/CD
│   ├── deploy-lambda.yml       # Lambda deployment
│   ├── deploy-glue.yml         # Glue deployment
│   ├── deploy-streamlit.yml    # Streamlit deployment
│   └── setup-athena.yml        # Athena setup
│
├── lambda/                     # Lambda ingestion function
│   ├── src/
│   │   └── index.py
│   └── requirements.txt
│
├── glue/                       # Glue ETL job
│   └── src/
│       └── transform_wistia_curated.py
│
├── streamlit/                  # Dashboard application
│   ├── app.py
│   ├── requirements.txt
│   └── .streamlit/
│       └── config.toml
│
├── config/                     # Configuration files
│   └── athena-ddl.sql
│
├── .gitignore
└── README.md
```

## CI/CD Workflow

### When you push changes:

1. **Lambda** (`lambda/src/index.py`)
   - ✅ Builds deployment package
   - ✅ Uploads to S3
   - ✅ Updates Lambda function
   - ✅ Tests with invoke

2. **Glue** (`glue/src/transform_wistia_curated.py`)
   - ✅ Uploads script to S3
   - ✅ Verifies configuration

3. **Streamlit** (`streamlit/app.py`)
   - ✅ Validates Python code
   - ✅ Checks dependencies
   - ✅ Deploys to Streamlit Cloud

4. **Athena** (`config/athena-ddl.sql`)
   - ✅ Creates/updates database
   - ✅ Verifies tables

## Making Changes

### Update Lambda function
```bash
# Edit lambda/src/index.py
git add lambda/src/index.py
git commit -m "Update ingestion logic"
git push
# GitHub Actions automatically deploys!
```

### Update Glue job
```bash
# Edit glue/src/transform_wistia_curated.py
git add glue/src/transform_wistia_curated.py
git commit -m "Update transformation"
git push
# GitHub Actions automatically deploys!
```

### Update Streamlit dashboard
```bash
# Edit streamlit/app.py
git add streamlit/app.py
git commit -m "Add new chart"
git push
# GitHub Actions automatically deploys!
```

### Update Athena tables
```bash
# Edit config/athena-ddl.sql
git add config/athena-ddl.sql
git commit -m "Add new table"
git push
# GitHub Actions automatically creates!
```

## Monitoring Deployments

1. Go to: `https://github.com/<username>/wistia-analytics-pipeline/actions`
2. Click on workflow run to see details
3. View logs for each step
4. Check for any failures and fix

## Local Development

### Install dependencies
```bash
cd streamlit
pip install -r requirements.txt
```

### Run Streamlit locally
```bash
streamlit run streamlit/app.py
```

Open browser to `http://localhost:8501`

### Test Lambda locally
```bash
cd lambda
pip install -r requirements.txt
python -m pytest tests/  # if tests exist
```

## Troubleshooting

### GitHub Actions failing?
1. Check GitHub Secrets are set: Settings → Secrets
2. Verify AWS credentials: `aws sts get-caller-identity`
3. Check AWS permissions for Lambda/Glue/Athena roles

### Dashboard not updating?
1. Verify Glue job ran: `aws glue list-job-runs --job-name wistia-transform-to-curated`
2. Check Athena data: Query in AWS Console
3. Restart Streamlit app from Streamlit Cloud dashboard

### Lambda not triggering?
1. Check EventBridge rule: `aws events describe-rule --name wistia-pipeline-daily-trigger`
2. Verify Lambda permissions in IAM
3. Check CloudWatch logs: `/aws/lambda/wistia-ingestion-lambda`

## Cost Estimation

| Service | Monthly Cost |
|---------|--------------|
| Lambda | $0.20 |
| Glue | $0.44/run × 30 = $13 |
| S3 | $1-2 |
| Athena | $3-10 |
| Streamlit Cloud | FREE |
| **Total** | **~$17-25** |

## Documentation

- [GitHub Setup Guide](./GITHUB_SETUP_GUIDE.md) - Step-by-step setup
- [Phase 1-5 Docs](./docs/) - Architecture and setup
- [Athena Guide](./docs/PHASE6A_ATHENA_SETUP.md) - Database setup
- [Streamlit Guide](./docs/PHASE6B_STREAMLIT_DEPLOYMENT.md) - Dashboard setup

## Next Steps

1. ✅ Set up GitHub repository
2. ✅ Configure GitHub Secrets
3. ✅ Push code to trigger workflows
4. ✅ Monitor deployments in Actions tab
5. ✅ Access Streamlit dashboard
6. 📈 Analyze Wistia data!

## Support

- **GitHub Actions:** https://docs.github.com/en/actions
- **AWS CLI:** https://docs.aws.amazon.com/cli/
- **Streamlit:** https://docs.streamlit.io
- **Athena:** https://docs.aws.amazon.com/athena/

## License

MIT License - see LICENSE file for details

---

**Happy analyzing!** 📊🚀
