# CODWH_BIN="$(pwd)/DerivedData/codwh/Build/Products/Debug/codwh"
CODWH_BIN="$(pwd)/src/exec_plan"
TEST_EXPECTED="$(pwd)/test-expected"
TEST_ACTUAL="$(pwd)/test-actual"
QUERIES="$(pwd)/queries"

# ANSI codes
A_BOLD="\033[1m"
A_RESET="\033[0m"
A_RED="\033[31m"

mkdir -p "$TEST_ACTUAL"

function printOutput() {
  cat "$TEST_ACTUAL/q$1"
}

echo -n "" > perf_results

for i in {1..9}; do
  echo Test $i
  $CODWH_BIN --server perf $i $QUERIES/q$i.ascii | tee -a perf_results
  $CODWH_BIN --server perf $i $QUERIES/q$i.ascii | tee -a perf_results
  $CODWH_BIN --server perf $i $QUERIES/q$i.ascii | tee -a perf_results
  echo ""
done

awk '
    {s+=$0}
    END {printf "Sum =%10.3f, Avg = %10.3f\n",s,s/NR}
' perf_results

rm perf_results
