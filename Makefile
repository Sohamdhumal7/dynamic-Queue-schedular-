# ════════════════════════════════════════════════════════════════
#  Makefile — Self-Optimizing Distributed Transaction Scheduler v2
# ════════════════════════════════════════════════════════════════

CXX      = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread
LIBS     = -lhiredis

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
    CXXFLAGS += -I/opt/homebrew/include
    LIBS     += -L/opt/homebrew/lib
endif
ifeq ($(OS), Windows_NT)
    CXX = "C:/Program Files/Microsoft Visual Studio/2022/Community/VC/Tools/MSVC/14.44.35207/bin/Hostx64/x64/cl.exe"
    CXXFLAGS = /std:c++17 /O2 /I"C:/project final/upgraded_scheduler/vcpkg/installed/x64-windows/include"
    LIBS = /link "C:/project final/upgraded_scheduler/vcpkg/installed/x64-windows/lib/hiredis.lib" /out:$@
endif

.PHONY: all scheduler_v2 worker_v2 clean dirs

all: dirs scheduler_v2 worker_v2
	@echo ""
	@echo "╔══════════════════════════════════════════╗"
	@echo "║  All v2 components built!                ║"
	@echo "╚══════════════════════════════════════════╝"

dirs:
	@mkdir -p metrics/static metrics/adaptive metrics/adaptive_lb
	@mkdir -p analysis/graphs_v2 logs

scheduler_v2: scheduler/scheduler_v2.cpp
	@echo "Building scheduler_v2..."
	$(CXX) $(CXXFLAGS) -o scheduler/scheduler_v2 scheduler/scheduler_v2.cpp $(LIBS)
	@echo "  ✔ scheduler/scheduler_v2"

worker_v2: worker/worker_v2.cpp
	@echo "Building worker_v2..."
	$(CXX) $(CXXFLAGS) -o worker/worker_v2 worker/worker_v2.cpp $(LIBS)
	@echo "  ✔ worker/worker_v2"

clean:
	rm -f scheduler/scheduler_v2 worker/worker_v2
	rm -f metrics/**/*.csv
	rm -f analysis/graphs_v2/*.png
	@echo "Clean done."
