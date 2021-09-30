#! /usr/local/bin/bash

  export LFTP_PASSWORD=`cat /Users/vickery/.lftpwd`
  if [[ `hostname` =~ cuny.edu ]]
  then /usr/local/bin/lftp -f ./getcunyrc
  else echo Cannot access Tumbleweed from `hostname`
       exit 1
  fi
  exit 0
