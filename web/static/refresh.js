// Vanilla, no dependencies. A meta refresh would work too, but it throws away
// scroll position and any half-typed filter, which is irritating on a page you
// leave open. This reloads only while the tab is visible: a dashboard forgotten
// in a background tab should not keep querying the database all weekend.
(function () {
  var box = document.getElementById('auto');
  if (!box) return;

  var KEY = 'queued.autorefresh';
  try {
    var saved = localStorage.getItem(KEY);
    if (saved !== null) box.checked = saved === '1';
  } catch (e) { /* private mode, or storage disabled: the default is fine */ }

  box.addEventListener('change', function () {
    try { localStorage.setItem(KEY, box.checked ? '1' : '0'); } catch (e) {}
  });

  setInterval(function () {
    if (box.checked && document.visibilityState === 'visible') {
      window.location.reload();
    }
  }, 5000);
})();
