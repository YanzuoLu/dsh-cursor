/*
 * AvailableModels field-number subset derived from can1357/oh-my-pi,
 * commit 650f3a108229ee11bf68ba3377f4e8c3391cd424,
 * packages/catalog/src/discovery/cursor-models.proto.
 *
 * MIT License
 * Copyright (c) 2025 Mario Zechner
 * Copyright (c) 2025-2026 Can Bölük
 * Copyright (c) 2026 Stencil Labs, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire'

export const CURSOR_AVAILABLE_MODELS_PATH = '/aiserver.v1.AiService/AvailableModels'

export function encodeAvailableModelsRequest() {
  return new BinaryWriter()
    .tag(2, 0).bool(true) // include_long_context_models
    .tag(5, 0).bool(true) // use_model_parameters
    .tag(7, 0).bool(true) // do_not_use_markdown
    .tag(12, 0).bool(true) // use_cloud_agent_effort_modes
    .finish()
}

// Only fields actually read by the catalog are decoded. Optional booleans stay
// absent when unreported; in particular an absent image flag is not true.
const parameterValue = { 1: ['id', 'string'], 2: ['value', 'string'] }
const booleanValue = {
  1: ['value', 'string'], 2: ['displayName', 'string'], 3: ['increasesModelCost', 'bool'],
  4: ['defaultBlockedInAdminAllowlist', 'bool'], 5: ['hideFromUserPickerWhenAdminBlocked', 'bool'],
  6: ['blockedByAdminAllowlist', 'bool'],
}
const enumValue = {
  1: ['value', 'string'], 2: ['displayName', 'string'], 3: ['increasesModelCost', 'bool'],
  4: ['blockedByAdminAllowlist', 'bool'],
}
const parameterType = {
  1: ['booleanParameter', { 1: ['values', booleanValue, true] }],
  2: ['enumParameter', { 1: ['values', enumValue, true] }],
}
const parameterDefinition = {
  1: ['id', 'string'], 2: ['name', 'string'], 4: ['parameterType', parameterType],
}
const variant = {
  1: ['parameterValues', parameterValue, true], 2: ['displayName', 'string'],
  3: ['isMaxMode', 'bool'], 4: ['isDefaultMaxConfig', 'bool'],
  5: ['isDefaultNonMaxConfig', 'bool'], 7: ['tagline', 'string'],
  8: ['displayNameOutsidePicker', 'string'], 9: ['variantStringRepresentation', 'string'],
  10: ['confirmationDialogue', { 4: ['blocksSubmission', 'bool'] }],
  11: ['legacySlug', 'string'],
}
const modelDetails = {
  1: ['name', 'string'], 2: ['defaultOn', 'bool'], 3: ['isLongContextOnly', 'bool'],
  4: ['isChatOnly', 'bool'], 5: ['supportsAgent', 'bool'], 6: ['degradationStatus', 'uint32'],
  9: ['supportsThinking', 'bool'], 10: ['supportsImages', 'bool'],
  14: ['supportsMaxMode', 'bool'], 15: ['contextTokenLimit', 'uint32'],
  16: ['contextTokenLimitForMaxMode', 'uint32'], 17: ['clientDisplayName', 'string'],
  18: ['serverModelName', 'string'], 19: ['supportsNonMaxMode', 'bool'],
  24: ['inputboxShortModelName', 'string'], 27: ['onlySupportsCmdK', 'bool'],
  29: ['parameterDefinitions', parameterDefinition, true], 30: ['variants', variant, true],
  35: ['isHidden', 'bool'], 36: ['legacySlugs', 'string', true],
  37: ['idAliases', 'string', true], 39: ['tagline', 'string'],
  43: ['defaultDisabledInAdminAllowlist', 'bool'], 46: ['requiresDataRetention', 'bool'],
}
const response = {
  1: ['modelNames', 'string', true], 2: ['models', modelDetails, true],
  11: ['useModelParameters', 'bool'],
}

function decode(bytes, schema) {
  const reader = new BinaryReader(bytes)
  const output = {}
  for (const field of Object.values(schema)) if (field[2]) output[field[0]] = []
  while (reader.pos < reader.len) {
    const [number, wire] = reader.tag()
    const field = schema[number]
    if (!field) {
      reader.skip(wire, number, 16)
      continue
    }
    const [name, type, repeated] = field
    const expected = type === 'bool' || type === 'uint32' ? 0 : 2
    if (wire !== expected) throw new Error('Invalid Cursor catalog protobuf field')
    const value = typeof type === 'object' ? decode(reader.bytes(), type) : reader[type]()
    if (repeated) output[name].push(value)
    else output[name] = value
  }
  return output
}

export function decodeAvailableModelsResponse(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.byteLength > 8 * 1024 * 1024) {
    throw new Error('Invalid Cursor catalog protobuf size')
  }
  return decode(bytes, response)
}
