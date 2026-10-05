package orchestrator

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// 计划 ID 与状态落点的安全错误：发起阶段即明确拒绝，绝不退化为多份运行共用状态。
var (
	// ErrPlanIDUnsafe 表示计划 ID 不适合作为状态落点（含 NUL、编码后超出文件系统
	// 单组件长度上限等）。拒绝发生在 Start 阶段，运行不会被发起。
	ErrPlanIDUnsafe = errors.New("plan id is unsafe for persistence")
	// ErrPlanLocationConflict 表示两个不同的计划 ID 映射到了同一个存储落点。
	// 合法配置下不会发生（默认落点编码是无碰撞单射）；自定义 StoreFactory
	// 返回相同落点标识时会在发起阶段被明确拒绝。
	ErrPlanLocationConflict = errors.New("plan state location conflict")
)

// maxStateFileNameLen 限制编码后状态文件名（含后缀）的字节长度，
// 留出余量以兼容常见文件系统单组件 255 字节上限与临时文件后缀。
const maxStateFileNameLen = 200

// stateFileSuffix 是每个计划状态文件的固定后缀。
const stateFileSuffix = ".state.json"

// encodePlanID 把任意计划 ID 可逆、无碰撞地编码为单个安全文件系统路径组件。
//
// 编码规则：仅保留 [A-Za-z0-9._-] 原样，其余每个字节一律按 "%XX" 大写
// 十六进制转义（'%' 自身也转义为 "%25"）。
//
// 该编码是单射：唯一的转义引导字符 '%' 本身被转义，因此两个不同的 ID
// 绝不可能编码出同一个文件名；编码结果只含安全字符，不含任何平台的路径
// 分隔符（/ 与 \ 都被转义），因而不可能逃逸状态目录，也不会产生 "."/".."
// 之外的特殊组件（而 "."/".." 加固定后缀后只是普通隐藏文件名）。
func encodePlanID(id string) string {
	var b strings.Builder
	b.Grow(len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// planStateLocation 返回计划在状态目录下的落点（文件绝对路径）与文件名，
// 对不适合落盘的计划 ID 返回 ErrPlanIDUnsafe，调用方必须在发起前拒绝。
func planStateLocation(dir, id string) (abs, name string, err error) {
	if strings.IndexByte(id, 0) >= 0 {
		return "", "", fmt.Errorf("%w: plan id %q contains NUL byte", ErrPlanIDUnsafe, id)
	}
	name = encodePlanID(id) + stateFileSuffix
	if len(name) > maxStateFileNameLen {
		return "", "", fmt.Errorf("%w: encoded state file name length %d exceeds limit %d (plan id=%q)",
			ErrPlanIDUnsafe, len(name), maxStateFileNameLen, id)
	}
	abs, err = filepath.Abs(filepath.Join(dir, name))
	if err != nil {
		return "", "", fmt.Errorf("%w: cannot resolve state path for %q: %v", ErrPlanIDUnsafe, id, err)
	}
	return abs, name, nil
}
