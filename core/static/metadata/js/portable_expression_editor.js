/*
 * elevata - Metadata-driven Data Platform Framework
 * Copyright © 2025-2026 Ilona Tag
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

(function() {
  "use strict";

  if (window.elevataPortableExpressionEditorLoaded) {
    if (window.elevataInitPortableExpressionEditors) {
      window.elevataInitPortableExpressionEditors(document);
    }
    return;
  }
  window.elevataPortableExpressionEditorLoaded = true;

  const VERSION = 1;
  const FUNCTIONS = {
    ABS: [1, 1],
    CEIL: [1, 1],
    FLOOR: [1, 1],
    LENGTH: [1, 1],
    LOWER: [1, 1],
    NULLIF: [2, 2],
    ROUND: [1, 2],
    TRIM: [1, 1],
    UPPER: [1, 1],
  };
  const PREDICATE_TYPES = ["comparison", "boolean", "not", "is_null", "between", "in"];
  const CAST_TYPES = [
    "BIGINT", "BINARY", "BOOLEAN", "DATE", "DECIMAL", "FLOAT",
    "INTEGER", "JSON", "STRING", "TIME", "TIMESTAMP", "UUID",
  ];
  const TYPE_GROUPS = [
    {label: "Value", types: ["column", "literal", "runtime_value"]},
    {label: "Transform", types: ["function", "arithmetic", "unary_arithmetic", "coalesce", "concat", "cast", "case"]},
    {label: "Condition", types: ["comparison", "boolean", "not", "is_null", "between", "in"]},
  ];

  function el(tag, attrs, text) {
    const node = document.createElement(tag);
    Object.entries(attrs || {}).forEach(([key, value]) => {
      if (key === "class") node.className = value;
      else if (key === "hidden") node.hidden = Boolean(value);
      else if (key.startsWith("data-")) node.setAttribute(key, value);
      else node[key] = value;
    });
    if (text !== undefined && text !== null) node.textContent = text;
    return node;
  }

  function defaultNode(type) {
    switch (type) {
      case "column": return {type: "column", name: ""};
      case "runtime_value": return {type: "runtime_value", name: "delta_cutoff"};
      case "literal": return {type: "literal", value: ""};
      case "arithmetic": return {type: "arithmetic", operator: "+", left: defaultNode("column"), right: defaultNode("literal")};
      case "unary_arithmetic": return {type: "unary_arithmetic", operator: "-", expression: defaultNode("column")};
      case "comparison": return {type: "comparison", operator: "=", left: defaultNode("column"), right: defaultNode("literal")};
      case "boolean": return {type: "boolean", operator: "AND", parts: [defaultNode("comparison"), defaultNode("comparison")]};
      case "not": return {type: "not", expression: defaultNode("comparison")};
      case "is_null": return {type: "is_null", expression: defaultNode("column"), negated: false};
      case "between": return {type: "between", expression: defaultNode("column"), lower: defaultNode("literal"), upper: defaultNode("literal"), negated: false};
      case "in": return {type: "in", expression: defaultNode("column"), values: [defaultNode("literal")], negated: false};
      case "case": return {type: "case", branches: [{when: defaultNode("comparison"), then: defaultNode("literal")}], else: null};
      case "function": return {type: "function", name: "UPPER", args: [defaultNode("column")]};
      case "coalesce": return {type: "coalesce", parts: [defaultNode("column"), defaultNode("literal")]};
      case "concat": return {type: "concat", parts: [defaultNode("column")]};
      case "cast": return {type: "cast", expression: defaultNode("column"), target_type: "STRING"};
      default: return defaultNode("column");
    }
  }

  function nodeLabel(type) {
    const labels = {
      column: "Upstream column",
      runtime_value: "Runtime value",
      literal: "Literal value",
      arithmetic: "Arithmetic",
      unary_arithmetic: "Unary arithmetic",
      comparison: "Comparison",
      boolean: "Boolean",
      not: "NOT",
      is_null: "NULL check",
      between: "BETWEEN",
      in: "IN",
      case: "CASE",
      function: "Function",
      coalesce: "COALESCE",
      concat: "CONCAT",
      cast: "CAST",
    };
    return labels[type] || type;
  }

  function appendTypeOptions(select, allowedTypes, selectedType, editor) {
    const allowed = new Set(allowedTypes || TYPE_GROUPS.flatMap(group => group.types));
    if (!editor.runtimeValues.length) allowed.delete("runtime_value");
    TYPE_GROUPS.forEach(group => {
      const types = group.types.filter(type => allowed.has(type));
      if (!types.length) return;
      const optgroup = el("optgroup", {label: group.label});
      types.forEach(type => {
        let label = nodeLabel(type);
        if (type === "column") label = editor.columnLabel;
        const option = el("option", {value: type}, label);
        option.selected = selectedType === type;
        optgroup.appendChild(option);
      });
      select.appendChild(optgroup);
    });
  }

  function typeSelect(node, onChange, allowedTypes, editor) {
    const select = el("select", {class: "form-select form-select-sm"});
    appendTypeOptions(select, allowedTypes, node.type, editor);
    select.addEventListener("change", () => onChange(defaultNode(select.value)));
    return select;
  }

  function initialTypeSelect(onChange, allowedTypes, editor) {
    const select = el("select", {class: "form-select form-select-sm"});
    const promptText = editor.predicateOnly ? "Choose filter condition..." : "Choose expression type...";
    const prompt = el("option", {value: "", disabled: true}, promptText);
    prompt.selected = true;
    select.appendChild(prompt);
    appendTypeOptions(select, allowedTypes, null, editor);
    select.addEventListener("change", () => {
      if (select.value) onChange(defaultNode(select.value));
    });
    return select;
  }

  function button(label, className, handler) {
    const b = el("button", {type: "button", class: className}, label);
    b.addEventListener("click", handler);
    return b;
  }

  function renderNode(editor, node, onReplace, depth, allowedTypes) {
    const card = el("div", {class: "border rounded p-2 mb-2 bg-body"});
    card.dataset.expressionDepth = String(depth);

    const head = el("div", {class: "d-flex align-items-center gap-2 mb-2"});
    const typeWrap = el("div", {class: "flex-grow-1"});
    typeWrap.appendChild(typeSelect(node, replacement => {
      Object.keys(node).forEach(key => delete node[key]);
      Object.assign(node, replacement);
      onReplace(node);
      editor.render();
    }, allowedTypes, editor));
    head.appendChild(typeWrap);
    card.appendChild(head);

    const body = el("div", {class: "ps-2"});
    const nested = (label, child, setter, childAllowedTypes) => {
      const wrap = el("div", {class: "mb-2"});
      wrap.appendChild(el("div", {class: "small fw-semibold mb-1"}, label));
      wrap.appendChild(renderNode(editor, child, replacement => {
        setter(replacement);
        editor.sync();
      }, depth + 1, childAllowedTypes));
      body.appendChild(wrap);
    };

    if (node.type === "column") {
      const columns = editor.selectedColumns();
      if (columns.length === 1) {
        node.name = columns[0];
        const label = el("div", {class: "small text-muted mb-1"}, "Selected " + editor.columnLabel.toLowerCase());
        const input = el("input", {
          type: "text",
          class: "form-control form-control-sm",
          value: node.name,
          readOnly: true,
        });
        body.appendChild(label);
        body.appendChild(input);
        editor.sync();
      } else if (columns.length > 1) {
        const label = el("div", {class: "small text-muted mb-1"}, editor.columnLabel);
        const select = el("select", {class: "form-select form-select-sm"});
        const prompt = el("option", {value: ""}, "Choose " + editor.columnLabel.toLowerCase() + "...");
        select.appendChild(prompt);
        columns.forEach(name => select.appendChild(el("option", {value: name}, name)));
        select.value = columns.includes(node.name) ? node.name : "";
        select.addEventListener("change", () => { node.name = select.value; editor.sync(); });
        body.appendChild(label);
        body.appendChild(select);
      } else {
        node.name = "";
        body.appendChild(el(
          "div",
          {class: "alert alert-secondary py-2 px-3 mb-0 small"},
          editor.noColumnsText
        ));
        editor.sync();
      }
    } else if (node.type === "runtime_value") {
      const label = el("div", {class: "small text-muted mb-1"}, "Runtime value");
      const select = el("select", {class: "form-select form-select-sm"});
      editor.runtimeValues.forEach(item => {
        select.appendChild(el("option", {value: item.name}, item.label));
      });
      if (!editor.runtimeValues.some(item => item.name === node.name) && editor.runtimeValues.length) {
        node.name = editor.runtimeValues[0].name;
      }
      select.value = node.name;
      select.addEventListener("change", () => { node.name = select.value; editor.sync(); });
      body.appendChild(label);
      body.appendChild(select);
    } else if (node.type === "literal") {
      const row = el("div", {class: "d-flex gap-2"});
      const kind = el("select", {class: "form-select form-select-sm", style: "max-width: 10rem"});
      const prompt = el("option", {value: "", disabled: true}, "Choose datatype...");
      kind.appendChild(prompt);
      ["text", "number", "boolean", "null"].forEach(k => kind.appendChild(el("option", {value: k}, k.charAt(0).toUpperCase() + k.slice(1))));
      const valueInput = el("input", {type: "text", class: "form-control form-control-sm"});
      let currentKind = "";
      if (node.value === null) currentKind = "null";
      else if (typeof node.value === "boolean") currentKind = "boolean";
      else if (typeof node.value === "number") currentKind = "number";
      else if (typeof node.value === "string" && node.value !== "") currentKind = "text";
      kind.value = currentKind;
      if (!currentKind) prompt.selected = true;
      valueInput.value = node.value === null ? "" : String(node.value);
      const updateLiteralInput = () => {
        valueInput.disabled = !kind.value || kind.value === "null";
        if (!kind.value) valueInput.placeholder = "Choose datatype first";
        else if (kind.value === "boolean") valueInput.placeholder = "true or false";
        else valueInput.placeholder = "value";
      };
      const applyLiteral = () => {
        updateLiteralInput();
        if (!kind.value) return;
        if (kind.value === "null") node.value = null;
        else if (kind.value === "boolean") node.value = String(valueInput.value).toLowerCase() === "true";
        else if (kind.value === "number") {
          const num = Number(valueInput.value);
          node.value = Number.isFinite(num) ? num : valueInput.value;
        } else node.value = valueInput.value;
        editor.sync();
      };
      kind.addEventListener("change", applyLiteral);
      valueInput.addEventListener("input", applyLiteral);
      updateLiteralInput();
      row.appendChild(kind);
      row.appendChild(valueInput);
      body.appendChild(row);
    } else if (node.type === "arithmetic") {
      const op = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 8rem"});
      ["+", "-", "*", "/", "%"].forEach(v => op.appendChild(el("option", {value: v}, v)));
      op.value = node.operator;
      op.addEventListener("change", () => { node.operator = op.value; editor.sync(); });
      body.appendChild(op);
      nested("Left", node.left, v => { node.left = v; });
      nested("Right", node.right, v => { node.right = v; });
    } else if (node.type === "unary_arithmetic") {
      const op = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 8rem"});
      ["+", "-"].forEach(v => op.appendChild(el("option", {value: v}, v)));
      op.value = node.operator;
      op.addEventListener("change", () => { node.operator = op.value; editor.sync(); });
      body.appendChild(op);
      nested("Expression", node.expression, v => { node.expression = v; });
    } else if (node.type === "comparison") {
      const op = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 10rem"});
      ["=", "!=", "<", "<=", ">", ">=", "LIKE"].forEach(v => op.appendChild(el("option", {value: v}, v)));
      op.value = node.operator;
      op.addEventListener("change", () => { node.operator = op.value; editor.sync(); });
      body.appendChild(op);
      nested("Left", node.left, v => { node.left = v; });
      nested("Right", node.right, v => { node.right = v; });
    } else if (node.type === "boolean") {
      const op = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 10rem"});
      ["AND", "OR"].forEach(v => op.appendChild(el("option", {value: v}, v)));
      op.value = node.operator;
      op.addEventListener("change", () => { node.operator = op.value; editor.sync(); });
      body.appendChild(op);
      renderList(editor, body, "Condition", node.parts, defaultNode("comparison"), 2, depth, () => editor.sync(), undefined, PREDICATE_TYPES);
    } else if (node.type === "not") {
      nested("Condition", node.expression, v => { node.expression = v; }, PREDICATE_TYPES);
    } else if (node.type === "is_null") {
      nested("Expression", node.expression, v => { node.expression = v; });
      body.appendChild(negatedToggle(node, editor, "IS NOT NULL"));
    } else if (node.type === "between") {
      nested("Expression", node.expression, v => { node.expression = v; });
      nested("Lower bound", node.lower, v => { node.lower = v; });
      nested("Upper bound", node.upper, v => { node.upper = v; });
      body.appendChild(negatedToggle(node, editor, "NOT BETWEEN"));
    } else if (node.type === "in") {
      nested("Expression", node.expression, v => { node.expression = v; });
      renderList(editor, body, "Value", node.values, defaultNode("literal"), 1, depth, () => editor.sync());
      body.appendChild(negatedToggle(node, editor, "NOT IN"));
    } else if (node.type === "function") {
      const fn = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 12rem"});
      Object.keys(FUNCTIONS).forEach(v => fn.appendChild(el("option", {value: v}, v)));
      fn.value = node.name;
      fn.addEventListener("change", () => {
        node.name = fn.value;
        const [min, max] = FUNCTIONS[node.name];
        while (node.args.length < min) node.args.push(defaultNode("column"));
        while (node.args.length > max) node.args.pop();
        editor.render();
      });
      body.appendChild(fn);
      const [minArgs, maxArgs] = FUNCTIONS[node.name] || [1, 1];
      renderList(editor, body, "Argument", node.args, defaultNode("column"), minArgs, depth, () => editor.sync(), maxArgs);
    } else if (node.type === "coalesce") {
      renderList(editor, body, "Part", node.parts, defaultNode("column"), 2, depth, () => editor.sync());
    } else if (node.type === "concat") {
      renderList(editor, body, "Part", node.parts, defaultNode("column"), 1, depth, () => editor.sync());
    } else if (node.type === "cast") {
      const target = el("select", {class: "form-select form-select-sm mb-2", style: "max-width: 12rem"});
      CAST_TYPES.forEach(v => target.appendChild(el("option", {value: v}, v)));
      target.value = node.target_type;
      target.addEventListener("change", () => { node.target_type = target.value; editor.sync(); });
      body.appendChild(target);
      nested("Expression", node.expression, v => { node.expression = v; });
    } else if (node.type === "case") {
      const branchesWrap = el("div", {class: "mb-2"});
      branchesWrap.appendChild(el("div", {class: "small fw-semibold mb-1"}, "WHEN / THEN branches"));
      node.branches.forEach((branch, index) => {
        const branchCard = el("div", {class: "border rounded p-2 mb-2"});
        branchCard.appendChild(el("div", {class: "small fw-semibold mb-1"}, "WHEN"));
        branchCard.appendChild(renderNode(editor, branch.when, v => { branch.when = v; editor.sync(); }, depth + 1, PREDICATE_TYPES));
        branchCard.appendChild(el("div", {class: "small fw-semibold mb-1"}, "THEN"));
        branchCard.appendChild(renderNode(editor, branch.then, v => { branch.then = v; editor.sync(); }, depth + 1));
        if (node.branches.length > 1) {
          branchCard.appendChild(button("Remove branch", "btn btn-outline-danger btn-sm", () => {
            node.branches.splice(index, 1);
            editor.render();
          }));
        }
        branchesWrap.appendChild(branchCard);
      });
      branchesWrap.appendChild(button("Add branch", "btn btn-outline-secondary btn-sm", () => {
        node.branches.push({when: defaultNode("comparison"), then: defaultNode("literal")});
        editor.render();
      }));
      body.appendChild(branchesWrap);

      const elseWrap = el("div", {class: "mb-2"});
      elseWrap.appendChild(el("div", {class: "small fw-semibold mb-1"}, "ELSE"));
      if (node.else) {
        elseWrap.appendChild(renderNode(editor, node.else, v => { node.else = v; editor.sync(); }, depth + 1));
        elseWrap.appendChild(button("Remove ELSE", "btn btn-outline-secondary btn-sm", () => { node.else = null; editor.render(); }));
      } else {
        elseWrap.appendChild(button("Add ELSE", "btn btn-outline-secondary btn-sm", () => { node.else = defaultNode("literal"); editor.render(); }));
      }
      body.appendChild(elseWrap);
    }

    card.appendChild(body);
    return card;
  }

  function negatedToggle(node, editor, label) {
    const wrap = el("div", {class: "form-check mt-2"});
    const input = el("input", {type: "checkbox", class: "form-check-input", checked: Boolean(node.negated)});
    const text = el("label", {class: "form-check-label"}, label);
    input.addEventListener("change", () => { node.negated = input.checked; editor.sync(); });
    wrap.appendChild(input);
    wrap.appendChild(text);
    return wrap;
  }

  function renderList(editor, parent, label, items, newItem, minimum, depth, onChange, maximum, allowedTypes) {
    items.forEach((item, index) => {
      const wrap = el("div", {class: "mb-2"});
      const title = el("div", {class: "d-flex justify-content-between align-items-center mb-1"});
      title.appendChild(el("span", {class: "small fw-semibold"}, label + " " + (index + 1)));
      if (items.length > minimum) {
        title.appendChild(button("Remove", "btn btn-outline-danger btn-sm py-0", () => {
          items.splice(index, 1);
          editor.render();
        }));
      }
      wrap.appendChild(title);
      wrap.appendChild(renderNode(editor, item, replacement => { items[index] = replacement; onChange(); }, depth + 1, allowedTypes));
      parent.appendChild(wrap);
    });
    if (maximum === undefined || items.length < maximum) {
      parent.appendChild(button("Add " + label.toLowerCase(), "btn btn-outline-secondary btn-sm", () => {
        items.push(JSON.parse(JSON.stringify(newItem)));
        editor.render();
      }));
    }
  }

  function humanSummary(node, editor) {
    if (!node) return "No portable expression defined.";
    switch (node.type) {
      case "column": return node.name ? editor.columnLabel + ": " + node.name : editor.columnLabel;
      case "literal": return "Literal";
      case "runtime_value": {
        const match = editor.runtimeValues.find(item => item.name === node.name);
        return match ? match.label : "Runtime value";
      }
      case "function": return (node.name || "Function") + "(...)";
      case "arithmetic": return "Arithmetic " + (node.operator || "");
      case "comparison": return "Comparison " + (node.operator || "");
      case "boolean": return "Boolean " + (node.operator || "");
      case "case": return "CASE expression";
      case "cast": return "CAST to " + (node.target_type || "type");
      case "coalesce": return "COALESCE expression";
      case "concat": return "CONCAT expression";
      default: return nodeLabel(node.type);
    }
  }

  class PortableExpressionEditor {
    constructor(root) {
      this.root = root;
      this.input = root.querySelector("[data-portable-expression-value]");
      this.workspace = root.querySelector("[data-expression-workspace]");
      this.summary = root.querySelector("[data-expression-summary]");
      this.clearButton = root.querySelector("[data-expression-clear]");
      this.startButton = root.querySelector("[data-expression-start]");
      this.clientError = root.querySelector("[data-expression-client-error]");
      this.id = "portable-expression-" + Math.random().toString(36).slice(2);
      this.editorKind = root.dataset.editorKind || "expression";
      this.predicateOnly = this.editorKind === "filter";
      this.columnLabel = root.dataset.columnLabel || "Upstream column";
      this.emptyText = root.dataset.emptyText || "No portable expression defined.";
      this.noColumnsText = this.columnLabel === "Source column"
        ? "No source columns are available for this SourceDataset yet."
        : "Select an upstream column first. The expression editor uses the selected lineage columns.";
      this.runtimeValues = this.readRuntimeValues();
      this.columns = this.readColumns();
      this.contract = this.readInitial();
      this.installDatalist();
      this.bind();
      if (this.contract) this.render();
    }

    readColumns() {
      try { return JSON.parse(this.root.dataset.columnNames || "[]"); }
      catch (_) { return []; }
    }

    readRuntimeValues() {
      try {
        const values = JSON.parse(this.root.dataset.runtimeValues || "[]");
        return Array.isArray(values) ? values.filter(item => item && item.name && item.label) : [];
      } catch (_) {
        return [];
      }
    }

    selectedColumns() {
      if (this.columnLabel === "Source column") {
        return Array.from(new Set(this.columns.filter(Boolean)));
      }
      const form = this.root.closest("form");
      const upstreamField = form ? form.querySelector('[name="upstream_columns"]') : null;
      if (upstreamField && upstreamField.selectedOptions) {
        const selected = Array.from(upstreamField.selectedOptions)
          .map(option => {
            const text = (option.textContent || "").trim();
            const parts = text.split(/\s*(?:·|:)\s*/);
            return (parts[parts.length - 1] || "").trim();
          })
          .filter(Boolean);
        if (selected.length) return Array.from(new Set(selected));
      }
      return Array.from(new Set(this.columns.filter(Boolean)));
    }

    readInitial() {
      const raw = (this.input.value || "").trim();
      if (!raw) return null;
      try {
        const parsed = JSON.parse(raw);
        if (parsed && parsed.version === VERSION && parsed.expression) return parsed;
      } catch (_) {
        this.showError("The stored portable expression could not be read by the editor.");
      }
      return null;
    }

    installDatalist() {
      const list = el("datalist", {id: this.id + "-columns"});
      this.columns.forEach(name => list.appendChild(el("option", {value: name})));
      this.root.appendChild(list);
    }

    bind() {
      this.startButton.addEventListener("click", () => {
        this.workspace.hidden = false;
        if (this.contract) this.render();
        else this.renderInitialChooser();
      });
      this.clearButton.addEventListener("click", () => {
        this.contract = null;
        this.input.value = "";
        this.workspace.replaceChildren();
        this.workspace.hidden = true;
        this.clearButton.hidden = true;
        this.startButton.textContent = "Add " + this.editorKind;
        this.summary.innerHTML = '<span class="text-muted">' + this.emptyText + '</span>';
        this.hideError();
      });
    }

    sync() {
      if (!this.contract) {
        this.input.value = "";
        return;
      }
      this.input.value = JSON.stringify(this.contract);
      this.summary.innerHTML = "";
      const badge = el("span", {class: "badge text-bg-success"}, "Portable: " + humanSummary(this.contract.expression, this));
      this.summary.appendChild(badge);
      this.clearButton.hidden = false;
      this.startButton.textContent = "Edit " + this.editorKind;
      this.hideError();
    }

    renderInitialChooser() {
      this.workspace.replaceChildren();
      const headingText = this.predicateOnly ? "Filter structure" : "Expression structure";
      const heading = el("div", {class: "small fw-semibold mb-2"}, headingText);
      this.workspace.appendChild(heading);
      const rootTypes = this.predicateOnly ? PREDICATE_TYPES : undefined;
      this.workspace.appendChild(initialTypeSelect(expression => {
        this.contract = {version: VERSION, expression};
        this.render();
      }, rootTypes, this));
      this.workspace.hidden = false;
    }

    render() {
      this.workspace.replaceChildren();
      if (!this.contract) {
        this.renderInitialChooser();
        return;
      }
      const headingText = this.predicateOnly ? "Filter structure" : "Expression structure";
      const heading = el("div", {class: "small fw-semibold mb-2"}, headingText);
      this.workspace.appendChild(heading);
      const rootTypes = this.predicateOnly ? PREDICATE_TYPES : undefined;
      this.workspace.appendChild(renderNode(this, this.contract.expression, replacement => {
        this.contract.expression = replacement;
        this.sync();
      }, 0, rootTypes));
      this.workspace.hidden = false;
      this.sync();
    }

    showError(message) {
      this.clientError.textContent = message;
      this.clientError.hidden = false;
    }

    hideError() {
      this.clientError.textContent = "";
      this.clientError.hidden = true;
    }
  }

  function init(root) {
    const scope = root || document;
    scope.querySelectorAll("[data-portable-expression-editor]").forEach(node => {
      if (node.dataset.portableExpressionBound === "1") return;
      node.dataset.portableExpressionBound = "1";
      new PortableExpressionEditor(node);
    });
  }

  window.elevataInitPortableExpressionEditors = init;
  document.addEventListener("DOMContentLoaded", () => init(document));
  if (document.body) {
    document.body.addEventListener("htmx:afterSwap", event => init(event.target));
  }
  init(document);
})();
