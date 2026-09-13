pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
      }
    }
    stage('Test') {
      steps {
        sh(script: 'cargo test')
      }
    }
    stage('Review') {
      steps {
        input message: 'Approve?'
      }
    }
  }
}
