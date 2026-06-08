(function () {
  function initSidebarResize() {
    var sidebar = document.querySelector(".sidebar");
    var resizer = document.getElementById("sidebar-resizer");
    var layout = document.querySelector(".layout");
    if (!sidebar || !resizer) {
      return;
    }

    var isDragging = false;

    function onMouseMove(event) {
      if (!isDragging) {
        return;
      }
      var minWidth = 220;
      var maxWidth = Math.floor(window.innerWidth * 0.33);
      var newWidth = Math.min(Math.max(event.clientX, minWidth), maxWidth);
      sidebar.style.width = String(newWidth) + "px";
      resizer.style.left = String(newWidth) + "px";
      if (layout) {
        layout.style.marginLeft = String(newWidth) + "px";
      }
    }

    function onMouseUp() {
      isDragging = false;
      document.body.style.userSelect = "";
    }

    resizer.addEventListener("mousedown", function () {
      if (window.innerWidth <= 900) {
        return;
      }
      isDragging = true;
      document.body.style.userSelect = "none";
    });

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
  }

  function setCollapsed(collapsed) {
    document.body.classList.toggle("sidebar-collapsed", collapsed);
    var button = document.getElementById("sidebar-toggle");
    if (button) {
      button.setAttribute("aria-expanded", String(!collapsed));
    }
  }

  function initSidebarToggle() {
    var button = document.getElementById("sidebar-toggle");
    if (!button) {
      return;
    }

    button.addEventListener("click", function () {
      var collapsed = !document.body.classList.contains("sidebar-collapsed");
      setCollapsed(collapsed);
    });

    setCollapsed(false);
  }

  function initTreeSectionSummaries() {
    // For schema/table tree-node links: navigate without toggling <details>,
    // preventing the flash of sub-sections collapsing on click.
    var nodeSummaries = document.querySelectorAll("details.tree-node > summary");
    nodeSummaries.forEach(function (summary) {
      summary.addEventListener("click", function (event) {
        var link = event.target.closest("a");
        if (link && summary.contains(link)) {
          event.preventDefault();
          window.location.href = link.href;
        }
      });
    });
  }

  function setColumnDetailsExpanded(toggleCell, expanded) {
    var targetId = toggleCell.getAttribute("data-column-details-target");
    if (!targetId) {
      return;
    }

    var detailsRow = document.getElementById(targetId);
    if (!detailsRow) {
      return;
    }

    detailsRow.hidden = !expanded;
    toggleCell.setAttribute("aria-expanded", String(expanded));
    toggleCell.setAttribute("title", expanded ? "Click to hide details." : "Click to show details.");
  }

  function initColumnDetailsControls() {
    function handleToggleCell(toggleCell) {
      var expanded = toggleCell.getAttribute("aria-expanded") === "true";
      setColumnDetailsExpanded(toggleCell, !expanded);
    }

    document.addEventListener("click", function (event) {
      var toggleCell = event.target.closest("td[data-column-details-target]");
      if (toggleCell) {
        handleToggleCell(toggleCell);
        return;
      }

      var actionButton = event.target.closest("button[data-column-details-action]");
      if (!actionButton) {
        return;
      }

      var action = actionButton.getAttribute("data-column-details-action");
      var panel = actionButton.closest(".panel");
      if (!panel) {
        return;
      }

      var toggleCells = panel.querySelectorAll("td[data-column-details-target]");
      toggleCells.forEach(function (cell) {
        if (action === "expand-all") {
          setColumnDetailsExpanded(cell, true);
        } else if (action === "collapse-all") {
          setColumnDetailsExpanded(cell, false);
        }
      });
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }

      var toggleCell = event.target.closest("td[data-column-details-target]");
      if (!toggleCell) {
        return;
      }

      event.preventDefault();
      handleToggleCell(toggleCell);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      initSidebarToggle();
      initSidebarResize();
      initTreeSectionSummaries();
      initColumnDetailsControls();
    });
  } else {
    initSidebarToggle();
    initSidebarResize();
    initTreeSectionSummaries();
    initColumnDetailsControls();
  }
})();
