
# AWS Rock Paper Scissors Pipeline

## Project Overview
This project implements a serverless AWS pipeline that simulates a Rock Paper Scissors game using Lambda, Glue, S3, SQS, and Streamlit.

## Architecture

```
Lambda (Generate Actions) 
    → S3 Landing Zone 
        → EventBridge Trigger 
            → Glue Job (Process Game) 
            → SQS Message
                → Streamlit Dashboard
```

## Project Structure

```
aws-rock-paper-scissors/
├── lambda/
│   └── action_generator.py
├── glue/
│   └── game_processor.py
├── streamlit/
│   └── app.py
├── cloudformation/
│   ├── nested-stack.yaml
│   ├── lambda-stack.yaml
│   ├── glue-stack.yaml
│   ├── s3-stack.yaml
│   └── sqs-stack.yaml
└── requirements.txt
```

## Quick Start

1. **Deploy Infrastructure:**
     ```bash
     aws cloudformation create-stack --stack-name rps-pipeline \
         --template-body file://cloudformation/nested-stack.yaml
     ```

2. **Deploy Streamlit:**
     ```bash
     streamlit run streamlit/app.py
     ```

---

