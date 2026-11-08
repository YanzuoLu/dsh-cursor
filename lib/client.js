(() => {
  // lib/provider-login-client.js
  function createProviderLoginClient(require2, { id, label, sectionId, order, apiPath }) {
    const React = require2("react");
    const { Button, Input } = require2("@deepseek-ai/dsh-client-ui-primitives");
    const { createElement: h, useCallback, useEffect, useRef, useState } = React;
    const grid = { display: "grid", gap: 12 };
    const row = { display: "flex", flexWrap: "wrap", gap: 8 };
    const paragraph = { margin: 0 };
    async function api(action, body, signal) {
      const response = await fetch(`${apiPath}/${action}`, {
        method: "POST",
        credentials: "same-origin",
        signal,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const result = await response.json();
      if (!response.ok || !result.ok) throw new Error(result.error?.message || `${label} \u767B\u5F55\u64CD\u4F5C\u5931\u8D25\uFF0C\u8BF7\u91CD\u8BD5\u3002`);
      return result.value;
    }
    function LoginPrompt({ prompt, busy, answer }) {
      const [input, setInput] = useState("");
      const inputId = `${sectionId}-prompt`;
      const submit = (event) => {
        event.preventDefault();
        if (busy || !input.trim()) return;
        const value = input;
        setInput("");
        answer(prompt.id, value);
      };
      return h(
        "div",
        { style: grid },
        h("label", { htmlFor: prompt.kind === "select" ? void 0 : inputId, style: paragraph }, prompt.message),
        prompt.kind === "select" ? h("div", { style: grid }, (prompt.options || []).map((option) => h(
          Button,
          {
            key: option.id,
            type: "button",
            variant: "outline",
            disabled: busy,
            onClick: () => {
              if (!busy && prompt.options.some((candidate) => candidate.id === option.id)) answer(prompt.id, option.id);
            }
          },
          h("span", null, option.label),
          option.description && h("span", { style: { marginLeft: 8, fontWeight: "normal" } }, option.description)
        ))) : h(
          "form",
          { onSubmit: submit, style: grid },
          h(Input, {
            id: inputId,
            type: prompt.kind === "secret" ? "password" : "text",
            value: input,
            disabled: busy,
            autoComplete: "off",
            spellCheck: false,
            placeholder: prompt.placeholder || "",
            onChange: (event) => setInput(event.target.value),
            style: { width: "100%" }
          }),
          h(Button, { type: "submit", variant: "primary", disabled: busy || !input.trim() }, "\u63D0\u4EA4")
        )
      );
    }
    function ProviderLoginSettings() {
      const [snapshot, setSnapshot] = useState(null);
      const [busy, setBusy] = useState(false);
      const [error, setError] = useState("");
      const [confirmLogout, setConfirmLogout] = useState(false);
      const [revision, setRevision] = useState(0);
      const sequence = useRef(0);
      const mounted = useRef(false);
      const busyRef = useRef(false);
      const activeRequest = useRef(null);
      const request = useCallback(async (action, body = {}, controller = new AbortController()) => {
        if (busyRef.current && action !== "cancel-login") return;
        const version = ++sequence.current;
        activeRequest.current?.abort();
        activeRequest.current = controller;
        const command = action !== "status";
        busyRef.current = command;
        setBusy(command);
        setError("");
        try {
          const value = await api(action, body, controller.signal);
          if (mounted.current && sequence.current === version && !controller.signal.aborted) setSnapshot(value);
        } catch (failure) {
          if (mounted.current && sequence.current === version && !controller.signal.aborted) setError(failure.message);
        } finally {
          if (mounted.current && sequence.current === version) {
            busyRef.current = false;
            setBusy(false);
            setRevision((value) => value + 1);
          }
        }
      }, []);
      useEffect(() => {
        mounted.current = true;
        void request("status");
        return () => {
          mounted.current = false;
          ++sequence.current;
          busyRef.current = false;
          activeRequest.current?.abort();
        };
      }, [request]);
      const login = snapshot?.login;
      const pending = login?.status === "pending";
      const checking = pending || Boolean(snapshot?.catalog?.loading);
      useEffect(() => {
        if (!checking || busy) return;
        const controller = new AbortController();
        const timer = setTimeout(() => {
          void request("status", {}, controller);
        }, 1500);
        return () => {
          clearTimeout(timer);
          controller.abort();
        };
      }, [checking, busy, revision, request]);
      const answer = (promptId, input) => {
        if (!pending || login.prompt?.id !== promptId) return;
        void request("answer", { promptId, input });
      };
      return h(
        "section",
        { style: { maxWidth: 640, ...grid, gap: 16 } },
        h("h2", { style: paragraph }, label),
        h("p", { style: paragraph }, "\u5728\u6D4F\u89C8\u5668\u767B\u5F55 Cursor\uFF0C\u51ED\u636E\u7531 DSH \u5728\u672C\u673A\u4FDD\u5B58\u3002\u4E0D\u4F1A\u8BFB\u53D6 Cursor \u684C\u9762\u6216 CLI \u767B\u5F55\u6587\u4EF6\u3002"),
        h("p", { style: paragraph }, "\u6B64\u4E3A\u975E\u5B98\u65B9\u79C1\u6709\u534F\u8BAE\u63A5\u5165\uFF0C\u5B58\u5728\u8D26\u53F7\u9650\u5236\u98CE\u9669\u3002\u4E0D\u4F1A\u81EA\u52A8\u6253\u5F00\u6D4F\u89C8\u5668\u6216\u5F00\u59CB\u767B\u5F55\u3002"),
        h("p", { role: "status", style: paragraph }, snapshot === null ? "\u6B63\u5728\u8BFB\u53D6\u767B\u5F55\u72B6\u6001\u2026" : snapshot.authenticated ? snapshot.credentialKind === "api-key" ? "\u5DF2\u4FDD\u5B58 API Key \u51ED\u636E" : `\u5DF2\u767B\u5F55${snapshot.expired ? "\uFF0C\u8BBF\u95EE\u4EE4\u724C\u5DF2\u8FC7\u671F\uFF0C\u4E0B\u6B21\u8BF7\u6C42\u5C06\u5C1D\u8BD5\u81EA\u52A8\u5237\u65B0\uFF1B\u5931\u8D25\u65F6\u8BF7\u91CD\u65B0\u767B\u5F55" : ""}` : "\u5C1A\u672A\u767B\u5F55"),
        snapshot?.authenticated && Number.isFinite(snapshot.expires) && h(
          "p",
          { style: { ...paragraph, fontSize: 13 } },
          `\u8BBF\u95EE\u4EE4\u724C\u6709\u6548\u671F\u81F3 ${new Date(snapshot.expires).toLocaleString()}`
        ),
        snapshot?.authenticated && snapshot.catalog && h("p", { style: paragraph }, snapshot.catalog.loading ? "\u6B63\u5728\u8BFB\u53D6\u6A21\u578B\u76EE\u5F55\u2026" : `\u53EF\u7528\u6A21\u578B ${snapshot.catalog.models} \u4E2A`),
        snapshot?.needsLogin && snapshot?.authenticated && h("p", { role: "alert", style: paragraph }, "\u6388\u6743\u5DF2\u5931\u6548\uFF0C\u8BF7\u91CD\u65B0\u767B\u5F55\u3002"),
        snapshot?.catalog?.error && h("p", { role: "alert", style: paragraph }, snapshot.catalog.error),
        snapshot?.available === false && h("p", { role: "alert", style: paragraph }, "DSH \u539F\u751F OAuth \u767B\u5F55\u5C1A\u672A\u5C31\u7EEA\uFF0C\u8BF7\u786E\u8BA4\u6B64\u63D0\u4F9B\u65B9\u5DF2\u542F\u7528\u3002"),
        snapshot?.inFlight && !pending && h("p", { role: "status", style: paragraph }, "\u5176\u4ED6\u754C\u9762\u6B63\u5728\u8FDB\u884C\u6B64\u63D0\u4F9B\u65B9\u7684\u767B\u5F55\uFF0C\u8BF7\u5148\u5B8C\u6210\u6216\u53D6\u6D88\u539F\u6765\u7684\u767B\u5F55\u3002"),
        h(
          "div",
          { style: row },
          !pending && h(Button, {
            type: "button",
            variant: "primary",
            disabled: busy || snapshot === null || snapshot.available === false || snapshot.inFlight,
            onClick: () => {
              setConfirmLogout(false);
              void request("start-login");
            }
          }, snapshot?.authenticated ? "\u91CD\u65B0\u767B\u5F55" : "\u767B\u5F55"),
          pending && h(Button, {
            type: "button",
            variant: "outline",
            onClick: () => {
              setConfirmLogout(false);
              void request("cancel-login");
            }
          }, "\u53D6\u6D88\u767B\u5F55"),
          snapshot?.authenticated && h(Button, {
            type: "button",
            variant: "outline",
            disabled: busy,
            onClick: () => setConfirmLogout(true)
          }, "\u9000\u51FA\u767B\u5F55"),
          h(Button, {
            type: "button",
            variant: "ghost",
            disabled: busy,
            onClick: () => {
              void request("status");
            }
          }, "\u5237\u65B0\u72B6\u6001"),
          snapshot?.authenticated && h(Button, {
            type: "button",
            variant: "outline",
            disabled: busy || pending || snapshot?.catalog?.loading,
            onClick: () => {
              void request("refresh-models");
            }
          }, "\u5237\u65B0\u6A21\u578B")
        ),
        confirmLogout && h(
          "div",
          { style: grid },
          h("p", { role: "alert", style: paragraph }, `\u9000\u51FA\u4F1A\u79FB\u9664\u5171\u4EAB\u7684 ${label} \u539F\u751F\u6388\u6743\uFF0C\u5F71\u54CD\u6240\u6709\u4F1A\u8BDD\u540E\u7EED\u5BF9\u6B64\u63D0\u4F9B\u5546\u7684\u6A21\u578B\u8BF7\u6C42\u3002\u786E\u5B9A\u9000\u51FA\u5417\uFF1F`),
          h(
            "div",
            { style: row },
            h(Button, {
              type: "button",
              variant: "primary",
              disabled: busy,
              onClick: () => {
                setConfirmLogout(false);
                void request("logout");
              }
            }, "\u786E\u8BA4\u9000\u51FA"),
            h(Button, {
              type: "button",
              variant: "ghost",
              disabled: busy,
              onClick: () => setConfirmLogout(false)
            }, "\u4FDD\u7559\u767B\u5F55")
          )
        ),
        pending && h(
          "div",
          { style: grid },
          h("p", { role: "status", style: paragraph }, "\u6B63\u5728\u7B49\u5F85\u539F\u751F\u767B\u5F55\u6D41\u7A0B\u5B8C\u6210\u2026"),
          login.notice && h("p", { style: { ...paragraph, whiteSpace: "pre-wrap" } }, login.notice),
          login.deviceCode && h(
            "div",
            { style: grid },
            h("label", { htmlFor: `${sectionId}-device-code` }, "\u8BBE\u5907\u9A8C\u8BC1\u7801\uFF0C\u53EF\u9009\u62E9\u5E76\u590D\u5236"),
            h(Input, {
              id: `${sectionId}-device-code`,
              type: "text",
              readOnly: true,
              value: login.deviceCode,
              style: { width: "100%" },
              onFocus: (event) => event.target.select()
            })
          ),
          login.authUrl && h("a", { href: login.authUrl, target: "_blank", rel: "noopener noreferrer" }, `\u6253\u5F00 ${label} \u767B\u5F55\u9875\u9762`),
          login.prompt && h(LoginPrompt, { key: login.prompt.id, prompt: login.prompt, busy, answer })
        ),
        login?.status === "complete" && h("p", { style: paragraph }, "\u767B\u5F55\u5B8C\u6210\u3002"),
        login?.status === "cancelled" && h("p", { style: paragraph }, "\u767B\u5F55\u5DF2\u53D6\u6D88\u3002"),
        (error || login?.error) && h("p", { role: "alert", style: { ...paragraph, whiteSpace: "pre-wrap" } }, error || login.error)
      );
    }
    return {
      inject: ["slots"],
      apply(ctx) {
        ctx.effect(() => ctx.slots.inject("settings.section", () => ctx.slots.register({
          name: "settings.section",
          id: sectionId,
          order,
          label: () => label
        }, ProviderLoginSettings)), `${id}: settings section`);
      }
    };
  }

  // client-entry.js
  window.__ModuleLoader__.load({
    id: "dsh-cursor",
    factory(require2) {
      return createProviderLoginClient(require2, {
        id: "dsh-cursor",
        label: "Cursor",
        sectionId: "cursor",
        order: 33,
        apiPath: "/api/dsh-cursor"
      });
    }
  });
})();
