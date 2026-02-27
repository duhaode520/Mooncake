#!/bin/bash
# Mooncake 本地编译打包脚本
# 用法: ./scripts/local_build.sh [python_version] [output_dir]
# 示例: ./scripts/local_build.sh 3.12 dist-py312

set -e

# 颜色定义
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m"

print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# 解析参数
PYTHON_VERSION=${1:-"3.12"}
OUTPUT_DIR=${2:-"dist-py${PYTHON_VERSION//./}"}

# 检测项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

echo -e "${YELLOW}Mooncake 本地编译打包脚本${NC}"
echo -e "项目目录: $ROOT_DIR"
echo -e "Python 版本: $PYTHON_VERSION"
echo -e "输出目录: mooncake-wheel/$OUTPUT_DIR"

# ============================================================
# 1. 检测 Python 环境
# ============================================================
print_section "检测 Python 环境"

# 优先使用 venv
if [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
    source "$ROOT_DIR/.venv/bin/activate"
    print_success "已激活 venv: $ROOT_DIR/.venv"
fi

# 检查 Python 版本
CURRENT_PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "")
if [ -z "$CURRENT_PYTHON_VERSION" ]; then
    print_error "未找到 Python，请先安装 Python $PYTHON_VERSION"
fi

if [ "$CURRENT_PYTHON_VERSION" != "$PYTHON_VERSION" ]; then
    print_warning "当前 Python 版本 ($CURRENT_PYTHON_VERSION) 与目标版本 ($PYTHON_VERSION) 不匹配"
    print_warning "继续使用当前版本构建..."
    PYTHON_VERSION=$CURRENT_PYTHON_VERSION
    OUTPUT_DIR="dist-py${PYTHON_VERSION//./}"
fi

print_success "Python 版本: $(python --version)"

# ============================================================
# 2. 检测 CUDA 环境
# ============================================================
print_section "检测 CUDA 环境"

USE_CUDA="OFF"
CUDA_PATH=""

# 查找 CUDA
for cuda_path in /usr/local/cuda /usr/local/cuda-12.8 /usr/local/cuda-12; do
    if [ -f "$cuda_path/bin/nvcc" ]; then
        CUDA_PATH="$cuda_path"
        break
    fi
done

if [ -n "$CUDA_PATH" ]; then
    export PATH="$CUDA_PATH/bin:$PATH"
    export LD_LIBRARY_PATH="$CUDA_PATH/lib64:$CUDA_PATH/lib64/stubs:$LD_LIBRARY_PATH"
    export LIBRARY_PATH="$CUDA_PATH/lib64/stubs:$LIBRARY_PATH"
    
    CUDA_VERSION=$(nvcc --version 2>/dev/null | grep -o "release [0-9][0-9]*\.[0-9]*" | awk '{print $2}' || echo "")
    if [ -n "$CUDA_VERSION" ]; then
        USE_CUDA="ON"
        print_success "检测到 CUDA $CUDA_VERSION (路径: $CUDA_PATH)"
    fi
else
    print_warning "未检测到 CUDA，将构建非 CUDA 版本"
fi

# 检测 GPU
if command -v nvidia-smi &> /dev/null; then
    GPU_INFO=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || echo "")
    if [ -n "$GPU_INFO" ]; then
        print_success "检测到 GPU: $GPU_INFO"
    fi
fi

# ============================================================
# 3. 清理并配置 CMake
# ============================================================
print_section "配置 CMake"

rm -rf build
mkdir -p build
cd build

CMAKE_ARGS=(
    -DBUILD_UNIT_TESTS=OFF
    -DUSE_HTTP=ON
    -DUSE_ETCD=ON
    -DUSE_CUDA=$USE_CUDA
    -DCMAKE_BUILD_TYPE=Release
)

echo "CMake 参数: ${CMAKE_ARGS[*]}"
cmake .. "${CMAKE_ARGS[@]}"

print_success "CMake 配置完成"

# ============================================================
# 4. 编译项目
# ============================================================
print_section "编译项目"

NPROC=$(nproc)
echo "使用 $NPROC 个并行任务编译..."
make -j$NPROC

print_success "项目编译完成"

# ============================================================
# 5. 构建 nvlink_allocator.so (仅 CUDA)
# ============================================================
if [ "$USE_CUDA" = "ON" ]; then
    print_section "构建 nvlink_allocator.so"
    
    mkdir -p "$ROOT_DIR/build/mooncake-transfer-engine/nvlink-allocator"
    cd "$ROOT_DIR/mooncake-transfer-engine/nvlink-allocator"
    
    if [ -f "build.sh" ]; then
        bash build.sh "$ROOT_DIR/build/mooncake-transfer-engine/nvlink-allocator/"
        print_success "nvlink_allocator.so 构建完成"
    else
        print_warning "未找到 nvlink-allocator/build.sh，跳过"
    fi
    
    cd "$ROOT_DIR"
fi

# ============================================================
# 6. 构建 Python Wheel
# ============================================================
print_section "构建 Python Wheel"

cd "$ROOT_DIR"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"

# 安装构建依赖
pip install --upgrade pip build setuptools wheel auditwheel -q

# 调用 build_wheel.sh
PYTHON_VERSION=$PYTHON_VERSION OUTPUT_DIR=$OUTPUT_DIR ./scripts/build_wheel.sh

# ============================================================
# 7. 显示结果
# ============================================================
print_section "构建完成"

WHEEL_FILE=$(ls -1 "$ROOT_DIR/mooncake-wheel/$OUTPUT_DIR/"*.whl 2>/dev/null | head -1)

if [ -f "$WHEEL_FILE" ]; then
    WHEEL_SIZE=$(du -h "$WHEEL_FILE" | cut -f1)
    echo -e "${GREEN}Wheel 文件: $WHEEL_FILE${NC}"
    echo -e "${GREEN}文件大小: $WHEEL_SIZE${NC}"
    
    echo -e "\n${BLUE}包含组件:${NC}"
    unzip -l "$WHEEL_FILE" 2>/dev/null | grep -E "\.(so|py)$" | awk '{print "  - " $4}' | head -20
    
    echo -e "\n${YELLOW}安装命令:${NC}"
    echo "  pip install $WHEEL_FILE"
else
    print_error "未找到生成的 wheel 文件"
fi

print_success "所有步骤完成！"
