rm -rf make_check.out || true
export GRIDDB_HOME=/home/hankh/work/pgspider_core_test/griddb_nosql-4.1.0/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/griddb/bin/
cd make_check_initializer
chmod +x ./*.sh || true
./init.sh
cd ..
#sed -i 's/REGRESS =.*/REGRESS = griddb_fdw griddb_fdw_data_type float4 float8 int4 int8 numeric join limit aggregates prepare select_having select insert update griddb_fdw_post /' Makefile
sed -i 's/REGRESS =.*/REGRESS = float4 float8 int4 int8 join limit aggregates prepare select_having select insert update griddb_fdw_post /' Makefile
make clean
make
make check | tee make_check.out
