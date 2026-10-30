import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { productCopy } from './gamedevbuddy-branding.mjs'

test('product copy is branded without changing transport, model or legal identities', () => {
  assert.equal(productCopy('HERMES AGENT'), 'Stumpy')
  assert.equal(productCopy('Welcome to Hermes Agent'), 'Welcome to Stumpy')
  assert.equal(productCopy('Open Hermes settings'), 'Open GameDevBuddy AI settings')
  for (const text of ['X-Hermes-Session-Token', 'hermes-gateway-v1', '@hermes/shared', 'https://hermes.example.test/api', 'Nous Hermes 4', 'Copyright NousResearch - all rights reserved']) {
    assert.equal(productCopy(text), text)
  }
})
