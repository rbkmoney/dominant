#!/bin/bash
cat <<EOF
version: '2'

services:
  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir: $PWD
    command: /sbin/init
    depends_on:
     - machinegun

  machinegun:
    image: dr.rbkmoney.com/rbkmoney/machinegun:e04e529f4c5682b527d12d73a13a3cf9eb296d4d
    volumes:
      - ./test/machinegun/sys.config:/opt/machinegun/releases/0.1.0/sys.config
    environment:
      - SERVICE_NAME=machinegun
EOF
