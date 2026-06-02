(() => {
  const stored = localStorage.getItem("starlight-theme");
  document.documentElement.dataset.theme = stored === "light" ? "light" : "dark";
})();
