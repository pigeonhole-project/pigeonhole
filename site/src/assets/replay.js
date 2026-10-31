// Plays a trace recorded by site/record.py into the #replay stage:
// commands in the terminal, chunks as posts in the two channels, the
// metadata log as a row of entries, and a sentence on what just happened.
(function () {
  const root = document.getElementById("replay");
  const trace = window.TRACE;
  if (!root || !trace) return;
  const stage = makeStage(root);
  const playBtn = root.querySelector('[data-act="play"]');
  const speedBtn = root.querySelector('[data-act="speed"]');

  const date = new Date(trace.recorded).toLocaleDateString(window.LANG, {day: "numeric", month: "long", year: "numeric"});
  root.querySelector('[data-role="meta"]').textContent = t("recorded", {commit: trace.commit, date});

  // One group per command: the command, what the server did, its output.
  const groups = [];
  for (const s of trace.steps) {
    if (s.type === "cmd") groups.push({cmd: s, events: [], out: null});
    else if (s.type === "out") groups.at(-1).out = s;
    else groups.at(-1).events.push(s);
  }

  let speed = 1, paused = false, run = 0, resume = null;
  function sleep(ms, id) {
    return new Promise(res => {
      const step = () => {
        if (id !== run) return;                  // restarted: drop this run
        if (paused) { resume = step; return; }
        res();
      };
      setTimeout(step, ms / speed);
    });
  }

  async function play() {
    const id = ++run;
    const wait = ms => sleep(ms, id);
    stage.reset();
    for (const g of groups) {
      const line = stage.termLine("cmd", "", g.cmd.t);
      const caret = h("span", "caret");
      line.append(caret);
      for (const ch of g.cmd.text) {
        caret.before(ch);
        await wait(22);
      }
      caret.remove();
      await wait(300);
      for (const e of g.events) {
        if (id !== run) return;
        await stage.draw(e, wait);
      }
      if (g.out && g.out.text) for (const l of g.out.text.split("\n")) stage.termLine("out", l, null);
      stage.narrate(sentence(g.events, g.cmd.text));
      await wait(g.events.some(e => e.type === "s3") || /\bls\b/.test(g.cmd.text) ? 3200 : 900);
    }
    stage.narrate(t("say.end"));
    await wait(7000);
    if (id === run) play();
  }

  playBtn.addEventListener("click", () => {
    paused = !paused;
    playBtn.textContent = paused ? t("play") : t("pause");
    if (!paused && resume) { const r = resume; resume = null; r(); }
  });
  root.querySelector('[data-act="restart"]').addEventListener("click", () => {
    paused = false; resume = null;
    playBtn.textContent = t("pause");
    play();
  });
  speedBtn.addEventListener("click", () => {
    speed = speed === 1 ? 2 : speed === 2 ? 0.5 : 1;
    speedBtn.textContent = `${speed}×`;
  });

  // Start when the stage scrolls into view; reduced motion starts paused.
  let started = false;
  new IntersectionObserver(es => {
    if (started || !es.some(e => e.isIntersecting)) return;
    started = true;
    if (matchMedia("(prefers-reduced-motion: reduce)").matches) {
      paused = true;
      playBtn.textContent = t("play");
    }
    play();
  }, {threshold: 0.25}).observe(root);
})();
