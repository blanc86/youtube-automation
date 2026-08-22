// ytauto local UI. Three small behaviours, no framework, no build step.
//
//  1. Copy the script prompt to the clipboard.
//  2. Start a render without navigating away, then poll until it ends.
//  3. Live-preview the two caption colours as they are picked.
//
// Everything degrades: with JavaScript off, the render form is still a plain
// POST and the settings form still submits - only the polling and the preview
// are lost.

(function () {
  "use strict";

  // -- copy button --------------------------------------------------------

  document.querySelectorAll("[data-copy-target]").forEach(function (button) {
    button.addEventListener("click", function () {
      var source = document.getElementById(button.dataset.copyTarget);
      if (!source) return;
      var restore = button.textContent;
      navigator.clipboard.writeText(source.textContent).then(
        function () {
          button.textContent = "Copied";
          setTimeout(function () {
            button.textContent = restore;
          }, 1600);
        },
        function () {
          // Clipboard access can be refused (a non-secure context, a
          // permission prompt declined). Selecting the text is the honest
          // fallback: the user can still press Ctrl-C.
          var range = document.createRange();
          range.selectNodeContents(source);
          var selection = window.getSelection();
          selection.removeAllRanges();
          selection.addRange(range);
          button.textContent = "Press Ctrl-C";
          setTimeout(function () {
            button.textContent = restore;
          }, 2400);
        }
      );
    });
  });

  // -- background task polling -------------------------------------------

  var POLL_MS = 1500;

  function statusUrl(panel, taskId) {
    return panel.dataset.pollUrl.replace("TASK_ID", taskId);
  }

  function spinnerRow(text) {
    var p = document.createElement("p");
    p.className = "status running";
    var spinner = document.createElement("span");
    spinner.className = "spinner";
    spinner.setAttribute("aria-hidden", "true");
    p.appendChild(spinner);
    p.appendChild(document.createTextNode(" " + text));
    return p;
  }

  function finish(panel, record) {
    var heading = panel.querySelector("h2");
    panel.textContent = "";
    if (heading) panel.appendChild(heading);

    var line = document.createElement("p");
    if (record.state === "succeeded") {
      line.className = "status ok";
      line.textContent = record.kind === "render" ? "Done. Both masters are in:" : record.detail;
      panel.appendChild(line);
      if (record.payload && record.payload.output_dir) {
        var dir = document.createElement("p");
        // The path is the single most important thing on this page - the user
        // has lost files before. textContent, not innerHTML: it is a real
        // filesystem path and must never be parsed as markup.
        dir.className = "output-dir";
        dir.textContent = record.payload.output_dir;
        panel.appendChild(dir);
      }
      if (record.kind === "broll") {
        // The table above this panel is now stale.
        setTimeout(function () {
          window.location.reload();
        }, 800);
      }
    } else {
      line.className = "status bad";
      line.textContent = record.detail || "It failed, with nothing to say about why.";
      panel.appendChild(line);
    }
  }

  function poll(panel, taskId) {
    fetch(statusUrl(panel, taskId), { headers: { Accept: "application/json" } })
      .then(function (response) {
        if (!response.ok) throw new Error("status " + response.status);
        return response.json();
      })
      .then(function (record) {
        if (!record.done) {
          setTimeout(function () {
            poll(panel, taskId);
          }, POLL_MS);
          return;
        }
        finish(panel, record);
      })
      .catch(function (err) {
        var line = document.createElement("p");
        line.className = "status bad";
        line.textContent = "Lost track of this task (" + err.message + "). Reload to check.";
        panel.appendChild(line);
      });
  }

  function showRunning(panel, runningText) {
    var heading = panel.querySelector("h2");
    panel.textContent = "";
    if (heading) panel.appendChild(heading);
    panel.appendChild(spinnerRow(runningText));
  }

  function watch(panel, taskId, runningText) {
    showRunning(panel, runningText);
    poll(panel, taskId);
  }

  document.querySelectorAll("[data-poll-url][data-task-id]").forEach(function (panel) {
    poll(panel, panel.dataset.taskId);
  });

  var renderForm = document.querySelector("#render-form");
  if (renderForm) {
    renderForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var button = renderForm.querySelector("button");
      var panel = document.querySelector("#render-status");
      button.disabled = true;
      showRunning(panel, "Starting…");
      fetch(renderForm.action, { method: "POST", headers: { Accept: "application/json" } })
        .then(function (response) {
          return response.json().then(function (body) {
            return { ok: response.ok, body: body };
          });
        })
        .then(function (result) {
          button.disabled = false;
          if (!result.ok) {
            finish(panel, { state: "failed", detail: result.body.error, kind: "render" });
            return;
          }
          watch(panel, result.body.id, "Rendering — this takes 10–120 seconds.");
        })
        .catch(function (err) {
          button.disabled = false;
          finish(panel, { state: "failed", detail: err.message, kind: "render" });
        });
    });
  }

  // -- caption colour preview --------------------------------------------

  var preview = document.querySelector("[data-caption-preview]");
  if (preview) {
    var primary = document.querySelector("[data-caption-primary]");
    var accent = document.querySelector("[data-caption-accent]");
    var word = preview.querySelector("[data-caption-word]");
    var apply = function () {
      preview.style.color = primary.value;
      word.style.color = accent.value;
    };
    primary.addEventListener("input", apply);
    accent.addEventListener("input", apply);
    apply();
  }
})();
