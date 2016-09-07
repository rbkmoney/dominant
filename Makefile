REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = damsel build_utils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := build_utils
TEMPLATES_PATH := .

SERVICE_NAME := dominant

# Service image default tag
SERVICE_IMAGE_TAG ?= $(shell git rev-parse HEAD)
# The tag for service image to be pushed with
SERVICE_IMAGE_PUSH_TAG ?= $(SERVICE_IMAGE_TAG)

# Base image for the service
BASE_IMAGE_NAME := service_erlang
BASE_IMAGE_TAG := 2202a02cbcb71982fea2e901ffb2b1ca5da610ae


## Variables required for utils_container.mk

# Build image tag to be used
BUILD_IMAGE_TAG := 753126790c9ecd763840d9fe58507335af02b875

BASE_IMAGE := "$(ORG_NAME)/build:latest"
RELNAME := dominant

TAG = latest
IMAGE_NAME = "$(ORG_NAME)/$(RELNAME):$(TAG)"

CALL_ANYWHERE := submodules rebar-update compile xref lint dialyze start devrel release clean distclean

CALL_W_CONTAINER := $(CALL_ANYWHERE) test

.PHONY: $(CALL_W_CONTAINER) all containerize push $(UTIL_TARGETS)

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk
-include $(UTILS_PATH)/make_lib/utils_image.mk

# CALL_ANYWHERE
$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

rebar-update:
	$(REBAR) update

compile: submodules rebar-update
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint: compile
	elvis rock

dialyze:
	$(REBAR) dialyzer

start: submodules
	$(REBAR) run

devrel: submodules
	$(REBAR) release

release: distclean
	$(REBAR) as prod release

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp

# CALL_W_CONTAINER
test: submodules
	$(REBAR) ct
