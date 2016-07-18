FROM rbkmoney/service_erlang:latest
MAINTAINER Igor Savchuk <i.savchuk@rbkmoney.com>
COPY _build/prod/rel/dmt /opt/dmt
CMD ["/opt/dmt/bin/dmt", "foreground"]
LABEL service_version="semver"
WORKDIR /opt/dmt