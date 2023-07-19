DOCKER_BUILD_CMD = docker build
detected_OS := $(shell sh -c 'uname 2>/dev/null || echo Unknown')
ifeq ($(detected_OS),Darwin)        # Mac OS X
    DOCKER_BUILD_CMD = docker buildx build --platform linux/amd64
endif

dev-requires:
	pip install -e .[dev]

test: dev-requires
	py.test --cov=dbt_cloud --cov-report html tests

version: 
  @dbt-cloud version