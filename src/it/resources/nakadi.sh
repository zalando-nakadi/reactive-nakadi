#!/bin/bash

# colors
OK="\033[32m"
FAIL="\033[31m"
RESET="\033[0m"

GIT=`which git`
DOCKER_COMPOSE=`which docker-compose`
DIRECTORY=/tmp/nakadi
REPO="https://github.com/zalando/nakadi"
NAKADI_PORT="8080"
NAKADI_ALIVE_TIMEOUT=240

function validate {

    [ -z "$DOCKER_IP" ] && {
        echo -e "You need to set DOCKER_IP env variable ${FAIL}✗${RESET}"
        exit 1;
    }

    [ "$DOCKER_COMPOSE" = "" ] && {
        echo -e "You need to install docker-compose ${FAIL}✗${RESET}"
        exit 1;
    }

    [ "$GIT" = "" ] && {
        echo "Git can't be found in your system"
        echo -ne "  ${OK}suggestion${RESET}: run '"
        [ x$(uname) = "xLinux" ] && {
            [ x$(which apt-get) != "x" ] && {
                echo "apt-get install git' to install it"
            } || {
                echo "yum install git' to install it"
            }
        } || {
            echo "brew install git' to install it"
        }
        echo ""
        exit 1
    }
}

function start_nakadi {
    echo -n "About to clone Nakadi... "
    rm -rf $DIRECTORY/
    $GIT clone $REPO $DIRECTORY
    echo -e "Cloned Nakadi to ${DIRECTORY} ${OK}✔${RESET}"

    echo -n "Editing some configs in $DIRECTORY... "
    sed  -i "" "s/localhost/$DOCKER_IP/g" $DIRECTORY/build.gradle
    sed  -i "" "s/localhost/$DOCKER_IP/g" $DIRECTORY/docker-compose.yml
    sed  -i "" "s/localhost/$DOCKER_IP/g" $DIRECTORY/src/main/resources/application.yml
    echo -e "Done! ${OK}✔${RESET}"

    echo -n "Building Nakadi... "
    cd $DIRECTORY/
    ./gradlew assemble
    $DOCKER_COMPOSE up -d
    cd -

    echo -n "Waiting on Nakadi to start (Polling http://$DOCKER_IP:8080/health) "
    poll_counter=0
    until $(curl --silent --output /dev/null http://$DOCKER_IP:8080/health); do
        sleep 1
        echo -n ". "
        poll_counter=$((poll_counter+1))
        [ "$poll_counter" -eq "$NAKADI_ALIVE_TIMEOUT" ] && {
            echo -e "Nakadi wait timeout reached ${FAIL}✗${RESET}"
            exit 1;
        }
    done;

    echo -e "Nakadi started ${OK}✔${RESET}"
}

function stop_nakadi {
    echo -n "Stopping Nakadi... "
    cd $DIRECTORY/
    ./gradlew stopNakadi
    cd -
    rm -rf $DIRECTORY/
    echo -e "Nakadi stopped ${OK}✔${RESET}"
}

function echo_usage {
    echo "Nakadi Docker helper script"
    echo $"Usage $0 {build-nakadi|remove-nakadi}"
    exit 1
}

case $1 in
    stop)
        validate
        stop_nakadi
        ;;
    start)
        validate
        start_nakadi
        ;;
    *)
        echo_usage
esac
