/** Create a settings section backed by the host's native provider OAuth controller. */
export function createProviderLoginClient(require, { id, label, sectionId, order, apiPath }) {
  const React = require('react');
  const { Button, Input } = require('@deepseek-ai/dsh-client-ui-primitives');
  const { createElement: h, useCallback, useEffect, useRef, useState } = React;
  const grid = { display: 'grid', gap: 12 };
  const row = { display: 'flex', flexWrap: 'wrap', gap: 8 };
  const paragraph = { margin: 0 };

  async function api(action, body, signal) {
    const response = await fetch(`${apiPath}/${action}`, {
      method: 'POST', credentials: 'same-origin', signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error?.message || `${label} 登录操作失败，请重试。`);
    return result.value;
  }

  // Remount for each prompt so an aborted or replaced native prompt retains no input.
  function LoginPrompt({ prompt, busy, answer }) {
    const [input, setInput] = useState('');
    const inputId = `${sectionId}-prompt`;
    const submit = event => {
      event.preventDefault();
      if (busy || !input.trim()) return;
      const value = input;
      setInput('');
      answer(prompt.id, value);
    };
    return h('div', { style: grid },
      h('label', { htmlFor: prompt.kind === 'select' ? undefined : inputId, style: paragraph }, prompt.message),
      prompt.kind === 'select'
        ? h('div', { style: grid }, (prompt.options || []).map(option =>
          h(Button, { key: option.id, type: 'button', variant: 'outline', disabled: busy,
            onClick: () => {
              if (!busy && prompt.options.some(candidate => candidate.id === option.id)) answer(prompt.id, option.id);
            } }, h('span', null, option.label),
          option.description && h('span', { style: { marginLeft: 8, fontWeight: 'normal' } }, option.description))))
        : h('form', { onSubmit: submit, style: grid },
          h(Input, { id: inputId, type: prompt.kind === 'secret' ? 'password' : 'text',
            value: input, disabled: busy, autoComplete: 'off', spellCheck: false,
            placeholder: prompt.placeholder || '', onChange: event => setInput(event.target.value),
            style: { width: '100%' } }),
          h(Button, { type: 'submit', variant: 'primary', disabled: busy || !input.trim() }, '提交')),
    );
  }

  function ProviderLoginSettings() {
    const [snapshot, setSnapshot] = useState(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState('');
    const [confirmLogout, setConfirmLogout] = useState(false);
    const [revision, setRevision] = useState(0);
    const sequence = useRef(0);
    const mounted = useRef(false);
    const busyRef = useRef(false);
    const activeRequest = useRef(null);
    const request = useCallback(async (action, body = {}, controller = new AbortController()) => {
      if (busyRef.current && action !== 'cancel-login') return;
      const version = ++sequence.current;
      activeRequest.current?.abort();
      activeRequest.current = controller;
      const command = action !== 'status';
      busyRef.current = command;
      setBusy(command);
      setError('');
      try {
        const value = await api(action, body, controller.signal);
        if (mounted.current && sequence.current === version && !controller.signal.aborted) setSnapshot(value);
      } catch (failure) {
        if (mounted.current && sequence.current === version && !controller.signal.aborted) setError(failure.message);
      } finally {
        if (mounted.current && sequence.current === version) {
          busyRef.current = false;
          setBusy(false);
          setRevision(value => value + 1);
        }
      }
    }, []);

    useEffect(() => {
      mounted.current = true;
      void request('status');
      return () => {
        mounted.current = false;
        ++sequence.current;
        busyRef.current = false;
        activeRequest.current?.abort();
      };
    }, [request]);

    const login = snapshot?.login;
    const pending = login?.status === 'pending';
    const checking = pending || Boolean(snapshot?.catalog?.loading);
    useEffect(() => {
      if (!checking || busy) return;
      const controller = new AbortController();
      const timer = setTimeout(() => { void request('status', {}, controller); }, 1500);
      return () => { clearTimeout(timer); controller.abort(); };
    }, [checking, busy, revision, request]);

    const answer = (promptId, input) => {
      if (!pending || login.prompt?.id !== promptId) return;
      void request('answer', { promptId, input });
    };
    return h('section', { style: { maxWidth: 640, ...grid, gap: 16 } },
      h('h2', { style: paragraph }, label),
      h('p', { style: paragraph }, '在浏览器登录 Cursor，凭据由 DSH 在本机保存。不会读取 Cursor 桌面或 CLI 登录文件。'),
      h('p', { style: paragraph }, '此为非官方私有协议接入，存在账号限制风险。不会自动打开浏览器或开始登录。'),
      h('p', { role: 'status', style: paragraph }, snapshot === null ? '正在读取登录状态…'
        : snapshot.authenticated ? snapshot.credentialKind === 'api-key' ? '已保存 API Key 凭据'
          : `已登录${snapshot.expired ? '，访问令牌已过期，下次请求将尝试自动刷新；失败时请重新登录' : ''}` : '尚未登录'),
      snapshot?.authenticated && Number.isFinite(snapshot.expires) && h('p', { style: { ...paragraph, fontSize: 13 } },
        `访问令牌有效期至 ${new Date(snapshot.expires).toLocaleString()}`),
      snapshot?.authenticated && snapshot.catalog && h('p', { style: paragraph }, snapshot.catalog.loading ? '正在读取模型目录…' : `可用模型 ${snapshot.catalog.models} 个`),
      snapshot?.needsLogin && snapshot?.authenticated && h('p', { role: 'alert', style: paragraph }, '授权已失效，请重新登录。'),
      snapshot?.catalog?.error && h('p', { role: 'alert', style: paragraph }, snapshot.catalog.error),
      snapshot?.available === false && h('p', { role: 'alert', style: paragraph }, 'DSH 原生 OAuth 登录尚未就绪，请确认此提供方已启用。'),
      snapshot?.inFlight && !pending && h('p', { role: 'status', style: paragraph }, '其他界面正在进行此提供方的登录，请先完成或取消原来的登录。'),
      h('div', { style: row },
        !pending && h(Button, { type: 'button', variant: 'primary', disabled: busy || snapshot === null || snapshot.available === false || snapshot.inFlight,
          onClick: () => { setConfirmLogout(false); void request('start-login'); } }, snapshot?.authenticated ? '重新登录' : '登录'),
        pending && h(Button, { type: 'button', variant: 'outline',
          onClick: () => { setConfirmLogout(false); void request('cancel-login'); } }, '取消登录'),
        snapshot?.authenticated && h(Button, { type: 'button', variant: 'outline', disabled: busy,
          onClick: () => setConfirmLogout(true) }, '退出登录'),
        h(Button, { type: 'button', variant: 'ghost', disabled: busy,
          onClick: () => { void request('status'); } }, '刷新状态'),
        snapshot?.authenticated && h(Button, { type: 'button', variant: 'outline', disabled: busy || pending || snapshot?.catalog?.loading,
          onClick: () => { void request('refresh-models'); } }, '刷新模型')),
      confirmLogout && h('div', { style: grid },
        h('p', { role: 'alert', style: paragraph }, `退出会移除共享的 ${label} 原生授权，影响所有会话后续对此提供商的模型请求。确定退出吗？`),
        h('div', { style: row },
          h(Button, { type: 'button', variant: 'primary', disabled: busy,
            onClick: () => { setConfirmLogout(false); void request('logout'); } }, '确认退出'),
          h(Button, { type: 'button', variant: 'ghost', disabled: busy,
            onClick: () => setConfirmLogout(false) }, '保留登录'))),
      pending && h('div', { style: grid },
        h('p', { role: 'status', style: paragraph }, '正在等待原生登录流程完成…'),
        login.notice && h('p', { style: { ...paragraph, whiteSpace: 'pre-wrap' } }, login.notice),
        login.deviceCode && h('div', { style: grid },
          h('label', { htmlFor: `${sectionId}-device-code` }, '设备验证码，可选择并复制'),
          h(Input, { id: `${sectionId}-device-code`, type: 'text', readOnly: true, value: login.deviceCode,
            style: { width: '100%' }, onFocus: event => event.target.select() })),
        login.authUrl && h('a', { href: login.authUrl, target: '_blank', rel: 'noopener noreferrer' }, `打开 ${label} 登录页面`),
        login.prompt && h(LoginPrompt, { key: login.prompt.id, prompt: login.prompt, busy, answer })),
      login?.status === 'complete' && h('p', { style: paragraph }, '登录完成。'),
      login?.status === 'cancelled' && h('p', { style: paragraph }, '登录已取消。'),
      (error || login?.error) && h('p', { role: 'alert', style: { ...paragraph, whiteSpace: 'pre-wrap' } }, error || login.error),
    );
  }

  return {
    inject: ['slots'],
    apply(ctx) {
      ctx.effect(() => ctx.slots.inject('settings.section', () => ctx.slots.register({
        name: 'settings.section', id: sectionId, order, label: () => label,
      }, ProviderLoginSettings)), `${id}: settings section`);
    },
  };
}
