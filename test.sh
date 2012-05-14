# CODWH_BIN="$(pwd)/DerivedData/codwh/Build/Products/Debug/codwh"
CODWH_BIN="$(pwd)/src/exec_plan"
TEST_EXPECTED="$(pwd)/test-expected"
TEST_ACTUAL="$(pwd)/test-actual"
QUERIES="$(pwd)/queries"

# ANSI codes
A_BOLD="\033[1m"
A_RESET="\033[0m"
A_RED="\033[31m"

(cd src/ &&  make)

mkdir -p "$TEST_ACTUAL"

function printOutput() {
  cat "$TEST_ACTUAL/q$1"
}

for i in {1..9}; do
  ($CODWH_BIN --server test $i $QUERIES/q$i.ascii > "$TEST_ACTUAL/q$i") || { printOutput $i; echo -e "\n"; }
  diff "$TEST_EXPECTED/q$i" "$TEST_ACTUAL/q$i" || { echo -e "\n Test($i) ${A_RED}FAILED${A_RESET}\n"; exit 1; }
done
echo -e "All tests ${A_BOLD}PASSED${A_RESET}"
