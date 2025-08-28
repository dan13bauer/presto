#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $0 <BUILD_DIR> <WORKER_CONFIG_DIR> <GPU_ID>"
    echo "Example $0 ./_build/release ~/worker2/etc/ 5"
    exit 1
fi

BUILD_DIR=$1
WORKER_CONFIG_DIR=$2
GPU_ID=$3

export LD_LIBRARY_PATH=/opt/rh/gcc-toolset-12/root/usr/lib64:/opt/rh/gcc-toolset-12/root/usr/lib:/usr/local/lib


UCX_PARAMS="UCX_TCP_CM_REUSEADDR=y UCX_TLS=^ib UCX_LOG_LEVEL=error UCX_TCP_KEEPINTVL=1ms UCX_KEEPALIVE_INTERVAL=1ms"
CUDA_PARAMS="CUDA_VISIBLE_DEVICES=$GPU_ID LIBCUDF_USE_DEBUG_STREAM_POOL=ON GLOG_logtostderr=1"
VELOX_ARGS="-velox_cudf_debug=true -velox_cudf_table_scan=true -velox_cudf_enabled=true -velox_cudf_exchange=true -velox_cudf_memory_resource=pool -v=3"


env $UCX_PARAMS env $CUDA_PARAMS $BUILD_DIR/presto_cpp/main/presto_server -etc_dir $WORKER_CONFIG_DIR $VELOX_ARGS
