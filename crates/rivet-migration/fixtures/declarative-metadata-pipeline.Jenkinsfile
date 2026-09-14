pipeline {
  agent any
  environment {
    BUILD_CHANNEL = 'nightly'
    RELEASE_TARGET = "staging"
  }
  parameters {
    string(name: 'TARGET', defaultValue: 'release', description: 'Deployment target')
    password(name: 'DEPLOY_TOKEN', description: 'Token supplied at run time')
  }
  stages {
    stage('Build') {
      steps {
        sh 'cargo build --release'
        archiveArtifacts artifacts: 'target/release/**, manifest.json', allowEmpty: true
      }
    }
  }
}
