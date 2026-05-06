#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Runs all GPU test binaries installed in /opt/gpu-tests/.
# Usage:
#   /opt/run-gpu-tests.sh [gtest_args...]
#
# Examples:
#   /opt/run-gpu-tests.sh
#   /opt/run-gpu-tests.sh --gtest_filter="*HashJoin*"
set -o pipefail

test_dir="/opt/gpu-tests"
failed=0
passed=0

if [ ! -d "$test_dir" ] || [ -z "$(ls -A "$test_dir" 2>/dev/null)" ]; then
  echo "No GPU test binaries found in $test_dir"
  echo "Was the image built with VELOX_BUILD_GPU_TESTS=ON?"
  exit 1
fi

for test_bin in "$test_dir"/*; do
  [ -x "$test_bin" ] || continue
  echo "=== Running $(basename "$test_bin") ==="
  "$test_bin" "$@"
  if [ $? -ne 0 ]; then
    failed=$((failed + 1))
  else
    passed=$((passed + 1))
  fi
  echo ""
done

echo "=== Results: $passed passed, $failed failed ==="
if [ $failed -ne 0 ]; then
  exit 1
fi
