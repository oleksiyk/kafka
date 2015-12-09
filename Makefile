all: riakfs

node_modules: package.json
	@npm install


riakfs: node_modules lib/*


#
# Tests
#
test: riakfs
	@mocha


#
# Coverage
#
lib-cov: clean-cov
	@jscoverage --no-highlight lib lib-cov

test-cov: lib-cov
	@RIAKFS_COV=1 mocha \
		--require ./test/globals \
		--reporter html-cov \
		> coverage.html

#
# Clean up
#

clean: clean-node clean-cov

clean-node:
	@rm -rf node_modules

clean-cov:
	@rm -rf lib-cov
	@rm -f coverage.html

.PHONY: all
.PHONY: test

