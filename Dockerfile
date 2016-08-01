FROM rbkmoney/service_erlang:latest
MAINTAINER Igor Savchuk <i.savchuk@rbkmoney.com>
COPY _build/prod/rel/dmt /opt/dominant
CMD ["/opt/dominant/bin/dmt", "foreground"]
LABEL service_version="semver"
WORKDIR /opt/dominant
