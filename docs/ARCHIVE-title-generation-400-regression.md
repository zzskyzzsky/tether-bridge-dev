Title: [Bug] Title generation fails with 400 on OpenAI-compatible endpoints that validate
       reasoning_effort as a strict enum (regressed in 0.21.3)

Status: archived evidence - NOT filed upstream (decision 2026-09-15 by project owner). Retained for future
reference; the sections below are issue-ready should that decision be revisited.

## Summary
After upgrading 0.20.0 -> 0.21.3 (tag v2026.9.14), auxiliary title generation fails on every new session:

  WARNING agent.title_generator: Title generation failed: Error code: 400 -
  {'error': {'message': 'Invalid option: expected one of "low"|"medium"|"high"|"xhigh"|"max"',
             'type': 'invalid_request_error', 'param': 'reasoning_effort'}}

Titles degrade gracefully - the session keeps an instant derived title (a slice of the user's actual words) -
and the main loop and other auxiliary tasks are unaffected. The failure is bounded at up to 2 failed calls per
new session (title_generator.py:316: "maybe_auto_title fires for the first two exchanges"), which is consistent with
the observed 3 and 2 occurrences.

## Steps to reproduce
1. Configure any OpenAI-compatible endpoint that validates reasoning_effort as a strict enum
   (low|medium|high|xhigh|max).
2. Use it as the current model (with no auxiliary.title_generation override the title task falls back to the
   main provider).
3. Start a new session and send one message (maybe_auto_title fires on the first two exchanges).
4. Observe in agent.log: WARNING agent.title_generator: Title generation failed: ... param: reasoning_effort.

## Regression (observed, not inferred)
Both machines were on 0.20.0 immediately before the upgrade (commit subject "chore: upgrade hermes-agent
0.20.0 -> 0.21.3 (v2026.9.14)"), with the same endpoint and model throughout.
- Machine A: WSL2 (kernel 6.6.87.2-microsoft-standard-WSL2), Ubuntu 24.04.1, Python 3.12.3.
  Rotated logs cover 2026-09-01 15:38 -> pre-upgrade on 2026-09-15: 0 occurrences.
  Immediately after the upgrade: 3 (16:46:02 / 16:48:32 / 17:02:19).
- Machine B: bare metal, Ubuntu 24.04.4, Python 3.11.15.
  Rotated logs cover 2026-09-03 00:50 -> pre-upgrade on 2026-09-15 10:23: 0 occurrences.
  Immediately after the upgrade: 2 (19:30:11 / 19:44:12).
=> ~26 machine-days with zero occurrences before the upgrade; 100% reproducible after it, on two different
   distro forms, filesystems and Python versions. Not environment-specific, not an endpoint change.
   Only title_generation is affected (other aux tasks show no failures in the same windows).

## Root cause (line numbers verified on two independent installs of the same tag)
1. agent/title_generator.py:309 hardcodes reasoning_config={"enabled": False} (added for #91927: the module
   contract promises thinking-disabled operation; otherwise providers that default thinking on bill thought
   tokens against max_tokens=64, the JSON payload never lands, and the prose fallback stores the opening fence
   as the title).
2. agent/transports/chat_completions.py:113-127 _reasoning_config_for_model() clamps only configs that carry
   an "effort" string:
       effort  = str(reasoning_config.get("effort") or "").strip().lower()
       clamped = clamp_effort(effort, OPENAI_COMPAT_WIRE_EFFORTS) if effort else effort
   => an "off" config ({"enabled": False}, no "effort" key) bypasses clamp_effort entirely and is passed
      through unchanged, and the existing clamp_effort helper (reasoning_effort.py:118-148) is exactly what
      should be reused here: with a supported set lacking none/minimal it returns the provider floor
      (e.g. low). Its docstring says "provider profiles with narrower sets clamp again downstream" - a custom
      (non-profile) provider has no such downstream clamp.
3. agent/reasoning_effort.py:28 OPENAI_COMPAT_WIRE_EFFORTS contains "none"/"minimal", and
   hermes_constants.py:930-940 parse_reasoning_effort() maps "none"/"false"/"disabled" to {"enabled": False}:
   Hermes' internal representation of "thinking off" is "none" (a member of its own wire vocabulary), while
   this endpoint's strict enum (low|medium|high|xhigh|max) has no equivalent value => "thinking off" has no
   legal wire representation on such endpoints. (We do not claim which value is emitted on the wire - see
   ## Scope note.)
4. Consistency argument: agent/reasoning_effort.py:151-153 requested_effort() documents that callers should
   OMIT the wire field when disabled; the title path sends a value instead, i.e. the implementation
   contradicts its own contract.
5. The failure depends on how strict the endpoint is (controlled A/B, same value reasoning_effort="none"):
   a permissive OpenAI-compatible endpoint returns 200; the strict-enum endpoint returns the 400 above.

## Suggested fixes (any, or a combination)
(a) Map the "off" semantics through the provider capability set by REUSING the existing clamp_effort helper
    (reasoning_effort.py:118-148): given a supported set without none/minimal it returns the provider floor
    (e.g. "low"). The helper already takes a declared vendor mapping via its `overrides` parameter (same hook
    used for e.g. Kimi K3 medium->high), so an OpenAI-compatible mapping fits the existing pattern rather than
    requiring new machinery.
(b) Omit the field when disabled - but this must not regress #91927: providers that default thinking on would
    overrun max_tokens=64 and store a JSON fence as the title. Gemini is already handled through the
    thinkingConfig path (chat_completions.py:129-142, called at :190 and :437); the OpenAI-compat path needs
    capability mapping (a) or capability-driven omission.
(c) Let custom OpenAI-compatible providers declare the effort levels they support (or a vendor mapping), so the
    downstream clamp the docstring promises ("provider profiles with narrower sets clamp again downstream")
    actually applies to them.

## Workaround
Titles are decorative: this failure only degrades them (the session keeps the instant derived title), so no
action is required. In an A/B test with the same value, a permissive OpenAI-compatible endpoint returned 200
where the strict-enum one returned 400, so routing the auxiliary title task to an endpoint that accepts the value
(one tolerating none/minimal) also avoids the warning.

## Scope note
We did not locate the exact line that emits the value onto the wire (in the generic branch, is_kimi / tokenhub /
lmstudio all explicitly check thinking_off and skip; _build_kwargs_from_profile was not followed). The report is
therefore "off semantics are not mapped + no capability entry for custom providers", not a named emit point.
All line numbers were verified line-by-line on two independent installs of tag v2026.9.14.

---- 中文附录 ----
现象：0.20.0 -> 0.21.3 后每个新会话的标题生成必报 400（param=reasoning_effort；该端点严格枚举
      low|medium|high|xhigh|max）。影响上限＝每会话最多 2 次失败调用（title_generator.py:316）；
      标题保留"用户原话切片"的即时降级标题；主链路与其它 auxiliary 任务不受影响。
回归：两端升级前均为 0.20.0（commit subject 为证）；升级前合共约 26 个机器日零命中，升级后 100% 复现。
根因：① title_generator.py:309 硬编码 {"enabled": False}（为 #91927 而加）；
      ② chat_completions.py:113-127 的 clamp 只处理"带 effort 字符串"的配置 ⇒ 关闭语义绕过 clamp；
         docstring 承诺"更窄的 provider profile 会在下游再 clamp"，而自定义 provider 没有 profile；
      ③ reasoning_effort.py:28 + hermes_constants.py:930-940 ⇒ Hermes 用 none 表示"关闭"，而该端点枚举不含
         none/minimal ⇒"关闭"语义在这类端点上没有合法的线上表示（不断言线上实际发出的值，见 Scope note）；
      ④ reasoning_effort.py:151-153 的契约写明"禁用时应省略线上字段"，与实现矛盾。
修法：优先 (a) 复用 clamp_effort（含 overrides 钩子）；(b) 会回归 #91927，须与 (a)/(c) 配套。
