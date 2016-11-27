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
    image: dr.rbkmoney.com/rbkmoney/machinegun:faa1156dd07a5cc72413616e3c73d48767654d3c
    volumes:
      - ./test/machinegun/sys.config:/opt/machinegun/releases/0.1.0/sys.config
    environment:
      - SERVICE_NAME=machinegun


networks:
  default:
    driver: bridge
    driver_opts:
      com.docker.network.enable_ipv6: "true"
      com.docker.network.bridge.enable_ip_masquerade: "true"
EOF
