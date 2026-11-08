import { createProviderLoginClient } from './lib/provider-login-client.js'

window.__ModuleLoader__.load({
  id: 'dsh-cursor',
  factory(require) {
    return createProviderLoginClient(require, {
      id: 'dsh-cursor', label: 'Cursor', sectionId: 'cursor', order: 33,
      apiPath: '/api/dsh-cursor',
    })
  },
})
