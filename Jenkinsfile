def dockerRepo = "ghcr.io/cancogen-virus-seq/muse"
def gitHubRepo = "cancogen-virus-seq/muse"
def chartVersion = "0.1.0"
def commit = "UNKNOWN"
def version = "UNKNOWN"

pipeline {
    agent {
        kubernetes {
            yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: jdk
    tty: true
    image: openjdk:11
    env:
      - name: DOCKER_HOST
        value: tcp://localhost:2375
    securityContext:
      runAsUser: 1000
      runAsGroup: 1000
      fsGroup: 1000
  - name: dind-daemon
    image: docker:18.06-dind
    securityContext:
      privileged: true
    env:
    - name: DOCKER_TLS_CERTDIR
      value: ''
    volumeMounts:
      - name: dind-storage
        mountPath: /var/lib/docker
  - name: helm
    image: alpine/helm:2.12.3
    command:
    - cat
    tty: true
  - name: docker
    image: docker:18-git
    command:
    - cat
    tty: true
    env:
    - name: DOCKER_HOST
      value: tcp://localhost:2375
    - name: HOME
      value: /home/jenkins/agent
    securityContext:
      runAsUser: 1000
      runAsGroup: 1000
      fsGroup: 1000
  volumes:
  - name: dind-storage
    emptyDir: {}

"""
        }
    }
    stages {
        stage('Prepare') {
            steps {
                script {
                    commit = sh(returnStdout: true, script: 'git describe --always').trim()
                }
                script {
                    version = readMavenPom().getVersion()
                }
            }
        }
        stage('Test') {
            steps {
                container('jdk') {
                    sh "./mvnw test"
                }
            }
        }
        stage('Build & Publish Develop') {
            when {
                branch "develop"
            }
            steps {
                container('docker') {
                    withCredentials([usernamePassword(credentialsId: 'argoContainers', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                        sh 'docker login ghcr.io -u $USERNAME -p $PASSWORD'
                    }

                    // DNS error if --network is default
                    sh "docker build --network=host . -t ${dockerRepo}:edge -t ${dockerRepo}:${version}-${commit}"

                    sh "docker push ${dockerRepo}:${version}-${commit}"
                    sh "docker push ${dockerRepo}:edge"
                }
            }
        }

       stage('deploy to cancogen-virus-seq-dev') {
           when {
               branch "develop"
           }
           steps {
               build(job: "virusseq/update-app-version", parameters: [
                   [$class: 'StringParameterValue', name: 'CANCOGEN_ENV', value: 'dev' ],
                   [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'muse'],
                   [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "${version}-${commit}" ]
               ])
           }
       }

        stage('Release & Tag') {
            when {
                branch "main"
            }
            steps {
                container('docker') {
                    withCredentials([usernamePassword(credentialsId: 'argoGithub', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh "git tag ${version}"
                        sh "git push https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${gitHubRepo} --tags"
                    }

                    withCredentials([usernamePassword(credentialsId: 'argoContainers', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                        sh 'docker login ghcr.io -u $USERNAME -p $PASSWORD'
                    }

                    // DNS error if --network is default
                    sh "docker build --network=host . -t ${dockerRepo}:latest -t ${dockerRepo}:${version}"

                    sh "docker push ${dockerRepo}:${version}"
                    sh "docker push ${dockerRepo}:latest"
                }
            }
        }

//        stage('deploy to cancogen-virus-seq-qa') {
//            when {
//                branch "main"
//            }
//            steps {
//                build(job: "/provision/helm", parameters: [
//                    [$class: 'StringParameterValue', name: 'AP_RDPC_ENV', value: 'qa' ],
//                    [$class: 'StringParameterValue', name: 'AP_CHART_NAME', value: 'muse'],
//                    [$class: 'StringParameterValue', name: 'AP_RELEASE_NAME', value: 'muse'],
//                    [$class: 'StringParameterValue', name: 'AP_HELM_CHART_VERSION', value: "${chartVersion}"],
//                    [$class: 'StringParameterValue', name: 'AP_ARGS_LINE', value: "--set-string image.tag=${version}" ]
//                ])
//            }
//        }
    }
}
