/* elevata - Metadata-driven Data Platform Framework
 * Copyright © 2025 Ilona Tag
 *
 * This file is part of elevata.
 *
 * elevata is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * elevata is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with elevata. If not, see <https://www.gnu.org/licenses/>.
 *
 * Contact: <https://github.com/elevata-labs/elevata>.
 */


// NOTE (2025-10-26):
// This used to prefill SourceDataset.stage_dataset.
// The staging dataset is now modeled as TargetDataset (target_schema = 'stage').
// This logic will be reused in the TargetDataset creation flow to suggest
// a canonical target_dataset name for the generated staging table.
// For now, this script is not actively invoked.


/* elevata - autofill for SourceDataset.stage_dataset
 * - Works on full page forms and HTMX inline row forms
 * - Fills stage_dataset as 'stg_<schema>_<name>' (schema omitted if empty or default)
 * - Lowercases, replaces spaces/special chars with '_', maps German umlauts
 * - Respects user override: once stage_dataset is edited, autofill stops
 */

(function () {
  "use strict";

  // Schemas that should be considered "default" (omit in name)
  var DEFAULT_SCHEMAS = ["dbo", "public", "default"];

  // Map German umlauts before stripping diacritics
  var UMLAUT_MAP = { "ä":"ae", "ö":"oe", "ü":"ue", "ß":"ss", "Ä":"Ae", "Ö":"Oe", "Ü":"Ue" };

  function mapUmlauts(s) {
    return s.replace(/[äöüÄÖÜß]/g, function (c) { return UMLAUT_MAP[c] || c; });
  }

  // Normalize to safe identifier: umlauts→ae/oe/ue/ss, remove diacritics, non-word→_, collapse _, lowercase
  function normalizeName(value) {
    if (!value) return "";
    var v = mapUmlauts(String(value));
    // strip other diacritics
    try { v = v.normalize("NFD").replace(/[\u0300-\u036f]/g, ""); } catch (_) { /* older browsers */ }
    v = v.replace(/[^a-zA-Z0-9_]+/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
    return v;
  }

  // Build suggested stage name
  function buildSuggestion(schemaVal, nameVal) {
    var base = normalizeName(nameVal);
    if (!base) return "";
    var schema = normalizeName(schemaVal || "");
    var useSchema = schema && DEFAULT_SCHEMAS.indexOf(schema) === -1;
    return (useSchema ? "stg_" + schema + "_" + base : "stg_" + base);
  }

  // Attach behavior to a single container (document or an HTMX-swapped row)
  function wireContainer(container) {
    // Support both "source_dataset" (correct) and "name" (fallback)
    var nameInput   = container.querySelector("#id_source_dataset") || container.querySelector("#id_name");
    var schemaInput = container.querySelector("#id_schema");
    var stageInput  = container.querySelector("#id_stage_dataset");
    if (!nameInput || !stageInput) return;

    // Once user edits stage manually, stop autofill
    var userEdited = false;

    function markUserEdited() { userEdited = true; }
    stageInput.addEventListener("input", markUserEdited);
    stageInput.addEventListener("change", markUserEdited);
    stageInput.addEventListener("keydown", markUserEdited);

    function updateSuggestion() {
      if (userEdited) return; // respect manual override
      var suggestion = buildSuggestion(schemaInput ? schemaInput.value : "", nameInput.value);
      if (!suggestion) return;
      // Only write if empty or already equal to previous suggestion pattern
      if (!stageInput.value || /^stg_/.test(stageInput.value)) {
        stageInput.value = suggestion;
      }
    }

    // Update on name changes (primary trigger)
    nameInput.addEventListener("input", updateSuggestion);
    // Also update when schema changes (if present)
    if (schemaInput) schemaInput.addEventListener("input", updateSuggestion);

    // Initial prefill if empty
    if (!stageInput.value) updateSuggestion();
  }

  // Full page load
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () { wireContainer(document); });
  } else {
    wireContainer(document);
  }

  // HTMX: whenever a row form is swapped in, wire it
  document.addEventListener("htmx:afterSwap", function (evt) {
    // Target is the swapped fragment; wire just that subtree
    try { wireContainer(evt.target); } catch (_) { /* no-op */ }
  });
})();
