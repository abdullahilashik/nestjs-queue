pipeline {
  agent any

  options {
    // Keep build logs timestamped and prevent concurrent deployments.
    timestamps()
    disableConcurrentBuilds()
  }

  tools {
    nodejs "NodeJS"
  }

  stages {
    stage('Install dependencies') {
      steps {
        // Install exactly the dependency versions recorded in the lockfile.
        sh 'npm ci'
      }
    }

    stage('Test, lint, and compile') {
      steps {
        // Fail the pipeline before creating an image if checks do not pass.
        // Jest needs Node's VM module support to load NestJS 12's ESM packages.
        sh 'NODE_OPTIONS=--experimental-vm-modules npm test -- --runInBand'
        sh 'npm run lint'
        sh 'npm run build'
      }
    }

    stage('Build Docker image') {
      steps {
        // Compose needs the Redis secret to validate the app's runtime config.
        withCredentials([string(credentialsId: 'redis-password', variable: 'REDIS_PASSWORD')]) {
          sh 'docker compose build app'
        }
      }
    }

    stage('Deploy') {
      when {
        // Automatically deploy only the main branch in a multibranch pipeline.
        branch 'main'
      }
      steps {
        // Run Compose on this Jenkins agent's Docker host using a stored secret.
        withCredentials([string(credentialsId: 'redis-password', variable: 'REDIS_PASSWORD')]) {
          sh 'docker compose up -d --no-build'
        }
      }
    }
  }
}
