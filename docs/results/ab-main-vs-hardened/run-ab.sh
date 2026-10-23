#!/bin/bash
SCR=/private/tmp/claude-501/-Users-guilhermelino/f84e6164-55df-4b35-9aa2-00ff48d4e49f/scratchpad
TIP=$HOME/repositories/agent-socket-tools
rm -f $SCR/ab/main-*.json $SCR/ab/main-*.md $SCR/ab/tip-*.json $SCR/ab/tip-*.md $SCR/ab/progress.txt
for i in 1 2 3; do
  for side in main tip; do
    if [ $side = main ]; then B=$SCR/mainbins/ast-bench; else B=$TIP/target/release/ast-bench; fi
    $B --only noop,concurrency --concurrency-levels 4,16,64 --repeats 7 --json-out $SCR/ab/$side-$i.json > $SCR/ab/$side-$i.md 2>$SCR/ab/$side-$i.err
    echo "run $i $side exit=$?" >> $SCR/ab/progress.txt
  done
done
echo ALLDONE >> $SCR/ab/progress.txt
