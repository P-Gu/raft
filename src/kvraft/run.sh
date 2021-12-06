for i in {1..20}
do
go test -run 3A -race
# |tee "run_$i.log"
done