package orchestrator

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// TestEncodePlanIDInjective 覆盖业务：多计划并行时，斜杠/空格/中文/点号
// 等历史上会相互碰撞的计划 ID，编码后必须两两不同，且全部落在状态目录内。
func TestEncodePlanIDInjective(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"plain", "plan-a"},
		{"slash", "env/a"},
		{"backslash", `env\a`},
		{"space", "env a"},
		{"chinese", "环境/计划 一"},
		{"dots", "a.b"},
		{"dot-only", "."},
		{"dotdot", ".."},
		{"percent", "a%2Fb"},
		{"underscore", "a_b"},
		{"mixed", "prod/us-east/订单 服务#1"},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := encodePlanID(tc.id)
			t.Logf("id=%q -> file component=%q", tc.id, enc)
			if prev, ok := seen[enc]; ok {
				t.Fatalf("encoding collision: %q and %q both map to %q", prev, tc.id, enc)
			}
			seen[enc] = tc.id
			// 编码结果绝不能携带路径分隔符，否则可能逃逸状态目录。
			if strings.ContainsAny(enc, `/\`) {
				t.Fatalf("encoded name %q still contains a path separator", enc)
			}
			// '%' 是唯一转义引导符：若原 ID 含 '%'，它必须自身被转义。
			if strings.ContainsRune(tc.id, '%') && enc == tc.id {
				t.Fatalf("percent sign not escaped in %q", enc)
			}
		})
	}

	// 历史碰撞对：旧的“非白名单字符一律替换为 _”会让下面这些 ID 落到同一文件。
	legacyColliders := []string{"a/b", `a\b`, "a b", "a.b"}
	names := map[string]bool{}
	for _, id := range legacyColliders {
		names[encodePlanID(id)] = true
	}
	if len(names) != len(legacyColliders) {
		t.Fatalf("legacy collider IDs still collide: %v", names)
	}
	t.Logf("legacy collider IDs encoded to %d distinct files: %v", len(names), names)
}

// TestPlanStateLocationContained 任意 ID 的落点都必须是状态目录下的单文件，
// 不允许出现目录逃逸（../ 等）。
func TestPlanStateLocationContained(t *testing.T) {
	dir := t.TempDir()
	ids := []string{"../../../etc/passwd", "a/../../b", "x/../y", "..", "环境/一"}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			abs, name, err := planStateLocation(dir, id)
			if err != nil {
				t.Fatalf("location for %q: %v", id, err)
			}
			t.Logf("id=%q -> %s", id, abs)
			rel, err := filepath.Rel(dir, abs)
			if err != nil || filepath.IsAbs(rel) {
				t.Fatalf("location %s cannot be made relative to %s: %v", abs, dir, err)
			}
			// 唯一合法的相对形态是目录内的一个单文件组件：
			// 不允许任何路径分隔，也不允许 "."/".." 之类的特殊目录组件。
			if filepath.Dir(rel) != "." || rel == "." || rel == ".." {
				t.Fatalf("location %s is not a single file inside %s (rel=%q)", abs, dir, rel)
			}
			if name != filepath.Base(abs) {
				t.Fatalf("location must be a single file component, got %s", abs)
			}
		})
	}
}

// TestPlanStateLocationUnsafe 含 NUL 或编码后超长的 ID 必须在发起前被拒绝。
func TestPlanStateLocationUnsafe(t *testing.T) {
	dir := t.TempDir()
	t.Run("nul byte", func(t *testing.T) {
		_, _, err := planStateLocation(dir, "bad\x00id")
		if !errors.Is(err, ErrPlanIDUnsafe) {
			t.Fatalf("want ErrPlanIDUnsafe for NUL id, got %v", err)
		}
		t.Logf("NUL id rejected: %v", err)
	})
	t.Run("too long", func(t *testing.T) {
		long := strings.Repeat("计", maxStateFileNameLen) // 每字节均转义为 %XX
		_, _, err := planStateLocation(dir, long)
		if !errors.Is(err, ErrPlanIDUnsafe) {
			t.Fatalf("want ErrPlanIDUnsafe for over-long id, got %v", err)
		}
		t.Logf("over-long id rejected: %v", err)
	})
	t.Run("safe long ascii accepted", func(t *testing.T) {
		id := strings.Repeat("a", maxStateFileNameLen-len(stateFileSuffix))
		_, _, err := planStateLocation(dir, id)
		if err != nil {
			t.Fatalf("id at exact length limit rejected: %v", err)
		}
	})
}
