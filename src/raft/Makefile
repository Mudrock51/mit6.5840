# write by kris4, just for easy debug
all: test3B run

test3A:
	@go test -race -run 3A > lab3a.log

test3B:
	@go test -race -run 3B > lab3b.log || true

run:
	@awk ' \
		/Passed/ { \
			print; \
		} \
		/FAIL/ { \
			print; \
			if (getline > 0) print; \
		} \
		/panic/ { \
			print; \
		} \
		/Test/ { \
			print; \
		}' lab3b.log
