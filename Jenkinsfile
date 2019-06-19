def MAIL_TO='$DEFAULT_RECIPIENTS'
def MAIL_SUBJECT='[CI PGSpider] GridDB FDW Test FAILED'

def retrySh(String shCmd) {
    script {
        int status = 1;
        for (int i = 0; i < 10; i++) {
            status = sh(returnStatus: true, script: shCmd)
            if (status == 0) {
                echo "SUCCESS: "+shCmd
                break
            } else {
                echo "RETRY: "+shCmd
                sleep 5
            }
        }
        if (status != 0) {
            sh(shCmd)
        }
    }
}

pipeline {
    agent {
        node {
            label 'AWS_CentOS_Instant'
        }
    }
    triggers { 
        pollSCM('H/30 * * * *') 
    }
    stages {
        stage('Build') { 
            steps {
                echo "Build PostgreSQL"
                sh '''
                    pwd
                    rm -rf postgresql-11.3
                    tar -zxvf /home/jenkins/fdw_test/postgresql.tar.gz > /dev/null
                '''
                dir("postgresql-11.3/contrib") { 
                    sh 'rm -rf griddb_fdw || true'
                    retrySh('git clone https://github.com/pgspider/griddb_fdw.git')
                }
            }
            post {
                failure {
                    echo '** BUILD FAILED !!! NEXT STAGE WILL BE SKIPPED **'
                    emailext subject: "${MAIL_SUBJECT}", body: '${BUILD_LOG, maxLines=200, escapeHtml=false}', to: "${MAIL_TO}", attachLog: false
                }
            }
        }
        stage('griddb_fdw_test') {
            steps {
                dir("postgresql-11.3/contrib/griddb_fdw") { 
                    catchError() {
                        sh 'cp -a /home/jenkins/GridDB/c_client_4.1.0/griddb ./'
                        sh 'chmod +x ./*.sh'
                    }
                    catchError() {
                        sh '''
                            rm -rf make_check.out || true
                            export GRIDDB_HOME=/home/jenkins/GridDB/griddb_nosql-4.1.0/
                            export LD_LIBRARY_PATH=LD_LIBRARY_PATH:$(pwd)/griddb/bin/
                            ./test.sh
                        '''
                    }
                    script {
                        status = sh(returnStatus: true, script: "grep -q 'All [0-9]* tests passed' 'make_check.out'")
                        if (status != 0) {
                            unstable(message: "Set UNSTABLE result")
                            emailext attachLog: false, body: '${FILE,path="make_check.out"}', subject: "${MAIL_SUBJECT}", to: "${MAIL_TO}"
                            catchError() {
                                sh 'cat regression.diffs'
                            }
                        }
                    }
                }
            }
        }
    }
}