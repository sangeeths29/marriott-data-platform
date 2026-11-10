// CI/CD only (scheduling lives in Airflow). Every branch: lint, unit + Spark/Iceberg
// integration tests, DAG integrity, dbt parse, terraform plan. main: deploy to dev
// automatically (infra + DAG sync to the MWAA bucket), prod after manual approval.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 45, unit: 'MINUTES') }
  environment {
    AWS_REGION = 'us-east-1'
    TF_IN_AUTOMATION = 'true'
  }

  stages {
    stage('Setup') {
      steps {
        sh '''
          python3 -m venv .venv
          . .venv/bin/activate
          pip install -q -r requirements.txt -r requirements-dev.txt
        '''
      }
    }

    stage('Lint') {
      steps { sh '. .venv/bin/activate && ruff check pipeline scripts tests airflow' }
    }

    stage('Tests') {
      parallel {
        stage('Unit + Spark/Iceberg') {
          steps { sh '. .venv/bin/activate && pytest -q --junitxml=reports/junit.xml' }
          post { always { junit 'reports/junit.xml' } }
        }
        stage('DAG integrity') {
          steps {
            sh '''
              python3 -m venv .venv-airflow && . .venv-airflow/bin/activate
              pip install -q pytest -r requirements-airflow.txt \\
                -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt
              AIRFLOW_HOME=$PWD/.airflow pytest -q tests/test_dags.py --junitxml=reports/junit-dags.xml
            '''
          }
          post { always { junit 'reports/junit-dags.xml' } }
        }
      }
    }

    stage('dbt compile') {
      steps { sh '. .venv/bin/activate && cd dbt && dbt parse --profiles-dir . --target local' }
    }

    stage('Package') {
      steps { sh 'make package' }
    }

    stage('Terraform plan (dev)') {
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-marriott-dp-dev']]) {
          dir('terraform') {
            sh '''
              terraform fmt -check -recursive
              terraform init -input=false
              terraform workspace select -or-create dev
              terraform validate
              terraform plan -input=false -var-file=envs/dev.tfvars -out=dev.tfplan
            '''
          }
        }
      }
    }

    stage('Deploy dev') {
      when { branch 'main' }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-marriott-dp-dev']]) {
          dir('terraform') { sh 'terraform apply -input=false dev.tfplan' }
          sh 'aws s3 sync airflow/dags "s3://${MWAA_DAGS_BUCKET_DEV}/dags" --delete --exclude "*/__pycache__/*"'
        }
      }
    }

    stage('Approve prod') {
      when { branch 'main' }
      steps {
        timeout(time: 1, unit: 'DAYS') {
          input message: 'Deploy to prod?', ok: 'Deploy'
        }
      }
    }

    stage('Deploy prod') {
      when { branch 'main' }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-marriott-dp-prod']]) {
          dir('terraform') {
            sh '''
              terraform workspace select -or-create prod
              terraform plan -input=false -var-file=envs/prod.tfvars -out=prod.tfplan
              terraform apply -input=false prod.tfplan
            '''
          }
        }
      }
    }
  }

  post {
    failure {
      echo 'Build failed - see console output. Configure email/Slack notification here.'
    }
    always { cleanWs(deleteDirs: true, notFailBuild: true) }
  }
}
