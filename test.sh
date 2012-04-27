# CODWH_BIN="$(pwd)/DerivedData/codwh/Build/Products/Debug/codwh"
CODWH_BIN="$(pwd)/src/exec_plan"
TEST_EXPECTED="$(pwd)/test-expected"
TEST_ACTUAL="$(pwd)/test-actual"
QUERIES="$(pwd)/queries"

mkdir -p "$TEST_ACTUAL"

function printOutput() {
  cat "$TEST_ACTUAL/q$1"
}

for i in {1..1}; do
  ($CODWH_BIN --server test $i $QUERIES/q$i.ascii > "$TEST_ACTUAL/q$i") || { printOutput $i; echo -e "\n"; }
  diff "$TEST_EXPECTED/q$i" "$TEST_ACTUAL/q$i" || { echo -e "\n Test($i) FAILED\n"; exit 1; }
done
