#!/bin/bash
cat <<EOF
FROM $BASE_IMAGE
MAINTAINER Igor Savchuk <i.savchuk@rbkmoney.com>
COPY _build/prod/rel/dominant /opt/dominant
CMD /opt/dominant/bin/dominant foreground
EXPOSE 8022
# A bit of magic below to get a proper branch name
# even when the HEAD is detached (Hey Jenkins!
# BRANCH_NAME is available in Jenkins env).
LABEL com.rbkmoney.$SERVICE_NAME.parent=$BASE_IMAGE_NAME \
      com.rbkmoney.$SERVICE_NAME.parent_tag=$BASE_IMAGE_TAG \
      com.rbkmoney.$SERVICE_NAME.build_img=build \
      com.rbkmoney.$SERVICE_NAME.build_img_tag=$BUILD_IMAGE_TAG \
      com.rbkmoney.$SERVICE_NAME.commit_id=$(git rev-parse HEAD) \
      com.rbkmoney.$SERVICE_NAME.commit_num=$(git rev-list --count HEAD) \
      com.rbkmoney.$SERVICE_NAME.branch=$( \
        if [ "HEAD" != $(git rev-parse --abbrev-ref HEAD) ]; then \
          echo $(git rev-parse --abbrev-ref HEAD); \
        elif [ -n "$BRANCH_NAME" ]; then \
          echo $BRANCH_NAME; \
        else \
          echo $(git name-rev --name-only HEAD); \
        fi)
WORKDIR /opt/dominant
EOF

