def NODE_NAME = 'AWS_Instance_CentOS'
def MAIL_TO = '$DEFAULT_RECIPIENTS'
def MAIL_SUBJECT = '[CI PGSpider] GridDB FDW Test FAILED'
def GRIDDB_FDW_URL = 'https://github.com/pgspider/griddb_fdw.git'
def GRIDDB_CLIENT_DIR = '/home/jenkins/GridDB/c_client_4.1.0/griddb'

def retrySh(String shCmd) {
    def MAX_RETRY = 10
    script {
        int status = 1;
        for (int i = 0; i < MAX_RETRY; i++) {
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
            label NODE_NAME
        }
    }
    triggers { 
        pollSCM('H/30 * * * *') 
    }
    stages {
        stage('Build') { 
            steps {
                sh '''
                    rm -rf postgresql || true
                    tar -zxvf /home/jenkins/Postgres/postgresql_release.tar.gz > /dev/null
                '''
                dir("postgresql/contrib") {
                    sh 'rm -rf griddb_fdw || true'
                    retrySh('git clone ' + GRIDDB_FDW_URL)
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
                dir("postgresql/contrib/griddb_fdw") { 
                    catchError() {
                        sh 'cp -a ' + GRIDDB_CLIENT_DIR + ' ./'
                        sh '''
                            rm -rf make_check.out || true
                            export GRIDDB_HOME=/home/jenkins/GridDB/griddb_nosql-4.1.0/
                            export LD_LIBRARY_PATH=LD_LIBRARY_PATH:$(pwd)/griddb/bin/
                            cd make_check_initializer
                            chmod +x ./*.sh || true
                            ./init.sh
                        '''
                        sh '''
                            sed -i 's/REGRESS =.*/REGRESS = griddb_fdw griddb_fdw_data_type float4 float8 int4 int8 numeric join limit aggregates prepare select_having select insert update griddb_fdw_post /' Makefile
                            make clean
                            make
                            make check | tee make_check.out
                        '''
                    }
                    script {
                        status = sh(returnStatus: true, script: "grep -q 'All [0-9]* tests passed' 'make_check.out'")
                        if (status != 0) {
                            unstable(message: "Set UNSTABLE result")
                            emailext subject: "${MAIL_SUBJECT}", body: '${FILE,path="make_check.out"}', to: "${MAIL_TO}", attachLog: false
                            sh 'cat regression.diffs || true'
                        }
                    }
                }
            }
        }
    }
}