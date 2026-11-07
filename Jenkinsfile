pipeline {
  agent any

  options {
    // Keep build logs timestamped and prevent concurrent deployments.
    timestamps()
    disableConcurrentBuilds()
  }

  stages {
    stage('Install dependencies') {
      steps {
        // Run Node 24 in its official image so the agent does not need a compatible host Node install.
        // Mount the workspace as the Jenkins user to keep generated files writable by Jenkins.
        sh 'docker run --rm --user "$(id -u):$(id -g)" --env npm_config_cache=/tmp/npm-cache --volume "$WORKSPACE:/workspace" --workdir /workspace node:24-bookworm npm ci'
      }
    }

    stage('Test, lint, and compile') {
      steps {
        // Fail before packaging if tests, lint, or the production TypeScript build fail.
        // Jest needs Node's VM module support to load NestJS 12's ESM packages.
        sh 'docker run --rm --user "$(id -u):$(id -g)" --env npm_config_cache=/tmp/npm-cache --volume "$WORKSPACE:/workspace" --workdir /workspace node:24-bookworm sh -c "NODE_OPTIONS=--experimental-vm-modules npm test -- --runInBand && npm run lint && npm run build"'
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
        // This Jenkins job checks out master; deploy only that branch.
        branch 'master'
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
