# all sqlite impl
PROGRAMS = mobibench_BC mobibench_2VTO mobibench_stock mobibench_DHL_simulate

# mobibench impl
BENCHMARK = main.c

# Benchmark notes:
#	For synthetic workloads:
#		- Disable the optimization flag -O3
# 	- To evaluate transaction size, please add compile flags `-DDHL_TEST_BATCH -DQUERIES_PER_TXN=4`
# For real workloads:
#		- Enable the optimization flag -O3
# 		- Because Google Play Service workload is a background service, please set "wal_close_period = -1"
#					i.e. wal force close can't be done by user's interaction.
#				On other workloads, leave it as it is in main.c ("wal_close_period = 100")
#		- Please find the traces in https://github.com/DICL/mobile-traces
BENCHMARK_RECOVERY = recovery.c

NDK=${HOME}/Workspace/android-ndk-r21e
# Only choose one of these, depending on your build machine...
TOOLCHAIN=${NDK}/toolchains/llvm/prebuilt/darwin-x86_64
# Only choose one of these, depending on your device...
export TARGET=armv7a-linux-androideabi
# Set this to your minSdkVersion.
API=24
# Configure and build.
AR=${TOOLCHAIN}/bin/llvm-ar
CC=${TOOLCHAIN}/bin/${TARGET}${API}-clang
AS=$CC
LD=${TOOLCHAIN}/bin/ld
RANLIB=${TOOLCHAIN}/bin/llvm-ranlib
STRIP=${TOOLCHAIN}/bin/llvm-strip

# compile flags
CFLAGS += -O3 -ldl -latomic -lm \
	-DHAVE_USLEEP=1 \
	-DSQLITE_HAVE_ISNAN \
	-DSQLITE_TEMP_STORE=3 \
	-DSQLITE_OMIT_LOOKASIDE \
	-DSQLITE_POWERSAFE_OVERWRITE=1 \
	-DSQLITE_DEFAULT_FILE_FORMAT=4 \
	-DSQLITE_DEFAULT_AUTOVACUUM=0 \
	-DSQLITE_OMIT_BUILTIN_TEST \
	-DSQLITE_OMIT_COMPILEOPTION_DIAGS \
	-DSQLITE_OMIT_LOAD_EXTENSION \
	-DSQLITE_DEFAULT_FILE_PERMISSIONS=0666 \
	-DSQLITE_DEFAULT_MEMSTATUS=0 \
	-DSQLITE_OMIT_INTEGRITY_CHECK \
	-DUSE_PREAD64 \
	-Dfdatasync=fdatasync \
	-DCVECTOR_LOGARITHMIC_GROWTH \
	-DGET_STATISTIC \
	--static

STOCK_CFLAGS 	= -DSQLITE_THREADSAFE=1 $(CFLAGS)

LSP_CFLAGS 		= -DSQLITE_THREADSAFE=2 $(CFLAGS)

all: $(PROGRAMS)

mobibench_DHL:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_DHL.c		$(STOCK_CFLAGS)

mobibench_DASH:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_DASH.c		$(STOCK_CFLAGS)

mobibench_stock:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_v3.7.c		$(STOCK_CFLAGS)

mobibench_2VTO:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_2VTO.c		$(LSP_CFLAGS)

mobibench_DHL_simulate:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_LSP.c		-DDHL_SIMULATION_PAGE_UTILIZATION $(LSP_CFLAGS)

mobibench_BC:
	${CC} -o $@		$(BENCHMARK)	../sources/sqlite3_BC.c			-DBEGIN_CONCURRENT	-DPRIVATE_CACHE	$(STOCK_CFLAGS)

recovery:
	rm -rf recovery_*
	${CC} -o recovery_vanilla		$(BENCHMARK_RECOVERY)	../sources/sqlite3_original.c	$(STOCK_CFLAGS)
	${CC} -o recovery_DHL				$(BENCHMARK_RECOVERY)	../sources/sqlite3_DHL.c			$(STOCK_CFLAGS)
	${CC} -o recovery_DASH			$(BENCHMARK_RECOVERY)	../sources/sqlite3_DASH.c			$(STOCK_CFLAGS)
	${CC} -o recovery_IPS				$(BENCHMARK_RECOVERY)	../sources/sqlite3_2VTO.c			$(LSP_CFLAGS)

gp:
	adb shell su -c rm -rf /sdcard/benchset
	adb push ../realdata/gp-loading.sql /sdcard/loading.sql
	adb push ../realdata/gp-benchset /sdcard/benchset
	adb shell su -c cp /sdcard/loading.sql /sbin
	adb shell su -c cp -r /sdcard/benchset /sbin

calendar:
	adb shell su -c rm -rf /sdcard/benchset
	adb push ../realdata/calendar-loading.sql /sdcard/loading.sql
	adb push ../realdata/calendar-benchset /sdcard/benchset
	adb shell su -c cp /sdcard/loading.sql /sbin
	adb shell su -c cp -r /sdcard/benchset /sbin

map:
	adb shell su -c rm -rf /sdcard/benchset
	adb push ../realdata/map-loading.sql /sdcard/loading.sql
	adb push ../realdata/map-benchset /sdcard/benchset
	adb shell su -c cp /sdcard/loading.sql /sbin
	adb shell su -c cp -r /sdcard/benchset /sbin

twitter:
	adb shell su -c rm -rf /sdcard/benchset
	adb push ../realdata/twitter-loading.sql /sdcard/loading.sql
	adb push ../realdata/twitter-benchset /sdcard/benchset
	adb shell su -c cp /sdcard/loading.sql /sbin
	adb shell su -c cp -r /sdcard/benchset /sbin

facebook:
	adb shell su -c rm -rf /sdcard/benchset
	adb push ../realdata/facebook-loading.sql /sdcard/loading.sql
	adb push ../realdata/facebook-benchset /sdcard/benchset
	adb shell su -c cp /sdcard/loading.sql /sbin
	adb shell su -c cp -r /sdcard/benchset /sbin

clean :
	@rm -rf mobibench_*
	rm -rf recovery_*

install:
	adb push mobibench_* /sdcard
	adb push *.sh /sdcard
	adb shell su -c cp /sdcard/mobibench_* /sbin
	adb shell su -c cp /sdcard/*.sh /sbin
	adb shell su -c chmod 755 /sbin/mobibench_*
	adb shell su -c chmod 755 /sbin/*.sh

install_recovery:
	adb push recovery_* /sdcard
	adb shell su -c cp /sdcard/recovery_* /sbin
	adb shell su -c chmod 755 /sbin/recovery_*
	adb push *.sh /sdcard
	adb shell su -c cp /sdcard/*.sh /sbin
	adb shell su -c chmod 755 /sbin/*.sh
