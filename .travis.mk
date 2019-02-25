#
# Travis CI rules
#

DOCKER_IMAGE:=packpack/packpack:debian-stretch

all: package

package:
	git clone https://github.com/packpack/packpack.git packpack
	./packpack/packpack

test: test_$(TRAVIS_OS_NAME)

# Redirect some targets via docker
test_linux:
	TYPE=docker_test_ubuntu make -f .travis.mk docker_test_ubuntu
coverage:
	TYPE=docker_coverage_ubuntu make -f .travis.mk docker_coverage_ubuntu

docker_common:
	mkdir -p ~/.cache/ccache
	docker run \
		--tty=true \
		--volume "${PWD}:/tarantool" \
		--name built_container_${TRAVIS_JOB_ID} \
		-e COVERALLS_TOKEN=${COVERALLS_TOKEN} \
		-e TRAVIS_JOB_ID=${TRAVIS_JOB_ID} \
		${DOCKER_IMAGE} \
		/bin/bash -c "cp -rfp /tarantool /tarantool_ws \
			&& cd /tarantool_ws \
			&& make -f .travis.mk $(subst docker_,,${TYPE}) \
			|| exit 1"
	docker tag ${DOCKER_IMAGE} ${DOCKER_IMAGE}_tmp
	docker commit built_container_${TRAVIS_JOB_ID} ${DOCKER_IMAGE}_tmp
	docker rm -f built_container_${TRAVIS_JOB_ID}
	cd test && suites=`ls -1 */suite.ini | sed 's#/.*##g'` ; cd .. ; \
	passed=0 ; \
	failed=0 ; \
	for suite in $$suites ; do \
		tests=`cd test/$$suite && ls -1 *.test.lua 2>/dev/null | sed 's#.test.lua##g'` ; \
		for test in $$tests ; do \
			TEST=$$suite/$$test.test ; \
			docker run \
				--rm=true --tty=true \
				--volume "${PWD}:/tarantool" \
				--workdir /tarantool_ws \
				-e COVERALLS_TOKEN=${COVERALLS_TOKEN} \
				-e TRAVIS_JOB_ID=${TRAVIS_JOB_ID} \
				-e TEST=$$TEST \
				${DOCKER_IMAGE}_tmp \
				/bin/bash -c "make -s -f .travis.mk $(subst docker_,run_,${TYPE}) \
					|| exit 1" \
				&& passed=$$(($$passed+1)) \
				|| failed=$$(($$failed+1)) ; \
		done ; \
	done ; \
	echo ; \
	echo "Overall results:" ; \
	echo "================" ; \
	echo "Passed # of tested scenarious: $$passed" ; \
	if [ "$$failed" -ne "0" ] ; then \
		echo "Failed # of tests: $$failed" ; \
		echo ------------------- ; \
		grep '\[ fail \]' ${PWD}/test_*.log | sed 's#.*:##g' ; \
		echo =================== ; \
		echo ; \
		false ; \
	fi

docker_test_ubuntu: docker_common
	# post test stage
	docker rmi -f ${DOCKER_IMAGE}_tmp

deps_ubuntu:
	sudo apt-get update \
		>/tarantool/apt_update.log 2>&1 \
		&& echo "APT update PASSED" \
		|| ( echo "APT update FAILED" ; \
			cat /tarantool/apt_update.log ; \
			exit 1 )
	sudo apt-get install -y -f \
		build-essential cmake coreutils sed \
		libreadline-dev libncurses5-dev libyaml-dev libssl-dev \
		libcurl4-openssl-dev libunwind-dev libicu-dev \
		python python-pip python-setuptools python-dev \
		python-msgpack python-yaml python-argparse python-six python-gevent \
		lcov ruby rsync \
		>/tarantool/apt_install.log 2>&1 \
		&& echo "APT install PASSED" \
		|| ( echo "APT install FAILED" ; \
			cat /tarantool/apt_install.log ; \
			exit 1 )

test_ubuntu: deps_ubuntu
	cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfoWError ${CMAKE_EXTRA_PARAMS} \
		>/tarantool/cmake.log 2>&1 \
		&& echo "CMAKE PASSED" \
		|| ( echo "CMAKE FAILED" ; \
			cat /tarantool/cmake.log ; \
			exit 1 )
	make -j \
		>/tarantool/make.log 2>&1 \
		&& echo "MAKE PASSED" \
		|| ( echo "MAKE FAILED" ; \
			cat /tarantool/make.log ; \
			exit 1 )

run_test_ubuntu:
	file="test_$(subst /,_,${TEST}).log" ; \
	sfile="/tarantool_ws/$$file" ; \
	cd test && /usr/bin/python test-run.py -j 1 ${TEST} \
		>$$sfile 2>&1 \
		&& ( echo "TEST(${TEST}) PASSED" ; \
			grep "Statistics:" -A1000 $$sfile | grep -v Statistics ) \
		|| ( echo "TEST(${TEST}) FAILED" ; \
			cat $$file ; \
			cp -f $$sfile /tarantool/. ; \
			exit 1 )
        
deps_osx:
	brew update
	brew install openssl readline curl icu4c --force

test_osx: deps_osx
	cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfoWError ${CMAKE_EXTRA_PARAMS}
	# Increase the maximum number of open file descriptors on macOS
	sudo sysctl -w kern.maxfiles=20480 || :
	sudo sysctl -w kern.maxfilesperproc=20480 || :
	sudo launchctl limit maxfiles 20480 || :
	ulimit -S -n 20480 || :
	ulimit -n
	make -j8
	virtualenv ./test-env && \
	. ./test-env/bin/activate && \
	curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | python && \
	pip --version && \
	pip install -r test-run/requirements.txt && \
	cd test && python test-run.py -j 1 unit/ app/ app-tap/ box/ box-tap/ && \
	deactivate

coverage_ubuntu: deps_ubuntu
	cmake . -DCMAKE_BUILD_TYPE=Debug -DENABLE_GCOV=ON \
		>/tarantool/cmake.log 2>&1 \
		&& echo "CMAKE PASSED" \
		|| ( echo "CMAKE FAILED" ; \
			cat /tarantool/cmake.log ; \
			exit 1 )
	make -j \
		>/tarantool/make.log 2>&1 \
		&& echo "MAKE PASSED" \
		|| ( echo "MAKE FAILED" ; \
			cat /tarantool/make.log ; \
			exit 1 )

run_coverage_ubuntu:
	# Enable --long tests for coverage
	file="test_$(subst /,_,${TEST}).log" ; \
	sfile="/tarantool_ws/$$file" ; \
	cd test && /usr/bin/python test-run.py -j 1 --long ${TEST} \
		>$$file 2>&1 \
		&& ( rsync -aunv /tarantool_ws /tarantool \
			echo "TEST(${TEST}) PASSED" ; \
			grep "Statistics:" -A1000 $$file | grep -v Statistics ) \
		|| ( echo "TEST(${TEST}) FAILED" ; \
			cat $$file ; \
			cp -f $$sfile /tarantool/. ; \
			exit 1 )

docker_coverage_ubuntu: docker_common
	# post test stage
	docker run \
		--rm=true --tty=true \
		--volume "${PWD}:/tarantool" \
		--workdir /tarantool_ws \
		-e COVERALLS_TOKEN=${COVERALLS_TOKEN} \
		-e TRAVIS_JOB_ID=${TRAVIS_JOB_ID} \
		${DOCKER_IMAGE}_tmp \
		/bin/bash -c "make -f .travis.mk analyze_coverage_ubuntu || exit 1"
	docker rmi -f ${DOCKER_IMAGE}_tmp

analyze_coverage_ubuntu:
	lcov --compat-libtool --directory src/ --capture --output-file coverage.info.tmp
	lcov --compat-libtool --remove coverage.info.tmp 'tests/*' 'third_party/*' '/usr/*' \
		--output-file coverage.info
	lcov --list coverage.info
	@if [ -n "$(COVERALLS_TOKEN)" ]; then \
		echo "Exporting code coverage information to coveralls.io"; \
		gem install coveralls-lcov; \
		echo coveralls-lcov --service-name travis-ci --service-job-id $(TRAVIS_JOB_ID) --repo-token [FILTERED] coverage.info; \
		coveralls-lcov --service-name travis-ci --service-job-id $(TRAVIS_JOB_ID) --repo-token $(COVERALLS_TOKEN) coverage.info; \
	fi

source:
	git clone https://github.com/packpack/packpack.git packpack
	TARBALL_COMPRESSOR=gz packpack/packpack tarball

# Push alpha and beta versions to <major>x bucket (say, 2x),
# stable to <major>.<minor> bucket (say, 2.2).
MAJOR_VERSION=$(word 1,$(subst ., ,$(TRAVIS_BRANCH)))
MINOR_VERSION=$(word 2,$(subst ., ,$(TRAVIS_BRANCH)))
BUCKET=tarantool.$(MAJOR_VERSION).$(MINOR_VERSION).src
ifeq ($(MINOR_VERSION),0)
BUCKET=tarantool.$(MAJOR_VERSION)x.src
endif
ifeq ($(MINOR_VERSION),1)
BUCKET=tarantool.$(MAJOR_VERSION)x.src
endif

source_deploy:
	pip install awscli --user
	aws --endpoint-url "${AWS_S3_ENDPOINT_URL}" s3 \
		cp build/*.tar.gz "s3://${BUCKET}/" \
		--acl public-read
