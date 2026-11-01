// The first visit: the setup step by step, the graph sketching it as it
// grows, then the pigeonhole.toml it makes. Start boots a node on it.
//
// makeWizard(panel, {sketch(channels), start(toml) → error | null}):
// panel: {title, sub, body} elements. Returns {render()}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {appOf} from "./channels.js";
import {divisors, perMinute, STEPS, sketch, tomlOf} from "./toml.js";

const MAX_CHANNELS = 4,
  MAX_BOTS = 4;
const numbers = n => Array.from({length: n}, (_, i) => [i + 1, String(i + 1)]);

export function makeWizard(panel, on) {
  const setup = {channels: ["telegram", "discord"], copies: 2, quorum: 2, log: "telegram", snapshots: "telegram", every: 50, bots: 1, pace: "demo"};
  let step = 0;
  let edited = null; // the toml as the reader changed it on the last step
  // A choice's effect: setup[key] = the value picked.
  const set = key => v => {
    setup[key] = v;
  };
  // New copies: a write waits for all of them, until the reader picks fewer.
  const setCopies = v => {
    setup.copies = setup.quorum = v;
  };

  // A row of choices, one of them on: radio buttons that look like a switch.
  function choices(label, options, value, pick) {
    const row = h("div", "choices");
    row.setAttribute("role", "radiogroup");
    row.setAttribute("aria-label", label);
    for (const [v, text] of options) {
      const b = button("choice", text, () => {
        pick(v);
        render();
      });
      b.setAttribute("role", "radio");
      b.setAttribute("aria-checked", String(v === value));
      row.append(b);
    }
    return row;
  }
  const kinds = (label, value, pick) =>
    choices(
      label,
      [
        ["telegram", "Telegram"],
        ["discord", "Discord"],
      ],
      value,
      pick,
    );
  const field = (label, control, note) => {
    const f = h("div", "field");
    f.append(h("div", "field-label", label), control);
    if (note) f.append(h("p", "field-note", note));
    return f;
  };

  // Copies must divide the channels: fewer channels, fewer copies.
  function fitCopies() {
    const n = setup.channels.length;
    if (n % setup.copies === 0) return;
    setup.copies = Math.max(...divisors(n).filter(d => d <= setup.copies));
    setup.quorum = setup.copies;
  }

  function copiesNote() {
    const n = setup.channels.length;
    if (n === 1) return t("wiz.one_channel");
    if (setup.copies === n) return t("wiz.all_copies", {n});
    if (setup.copies === 1) return t("wiz.all_shards", {n});
    return t("wiz.mixed", {shards: n / setup.copies, copies: setup.copies});
  }

  const steps = {
    channels() {
      const out = [h("p", "lede", t("wiz.channels_lede"))];
      setup.channels.forEach((kind, i) => {
        const row = h("div", "chan-row"),
          name = t("wiz.channel_n", {n: i + 1});
        row.append(
          h("span", "chan-n", name),
          kinds(name, kind, v => {
            setup.channels[i] = v;
          }),
        );
        if (setup.channels.length > 1) {
          row.append(
            button("mini", t("wiz.remove"), () => {
              setup.channels.splice(i, 1);
              fitCopies();
              render();
            }),
          );
        }
        out.push(row);
      });
      if (setup.channels.length < MAX_CHANNELS) {
        out.push(
          button("btn", t("wiz.add_channel"), () => {
            setup.channels.push(setup.channels.at(-1) === "telegram" ? "discord" : "telegram");
            fitCopies();
            render();
          }),
        );
      }
      return out;
    },
    copies() {
      const out = [h("p", "lede", t("wiz.copies_lede"))];
      const opts = divisors(setup.channels.length).map(d => [d, String(d)]);
      out.push(field(t("wiz.copies"), choices(t("wiz.copies"), opts, setup.copies, setCopies), copiesNote()));
      if (setup.copies > 1) {
        const q = choices(t("wiz.quorum"), numbers(setup.copies), setup.quorum, set("quorum"));
        out.push(field(t("wiz.quorum"), q, t("wiz.quorum_note", {q: setup.quorum, n: setup.copies})));
      }
      return out;
    },
    meta() {
      const every = h("input", "num");
      Object.assign(every, {type: "number", min: 5, max: 1000, step: 5, value: setup.every});
      every.addEventListener("change", () => {
        setup.every = Math.max(5, Math.min(1000, Math.round(+every.value) || 50));
        every.value = setup.every;
      });
      return [
        h("p", "lede", t("wiz.meta_lede")),
        field(t("wiz.log"), kinds(t("wiz.log"), setup.log, set("log")), t("wiz.log_note")),
        field(t("wiz.snapshots"), kinds(t("wiz.snapshots"), setup.snapshots, set("snapshots")), t("wiz.snapshots_note")),
        field(t("wiz.every"), every, t("wiz.every_note")),
      ];
    },
    bots() {
      const pace = [
        ["real", t("wiz.pace_real")],
        ["demo", t("wiz.pace_demo")],
      ];
      const rates = h("ul", "rates");
      for (const k of new Set([...setup.channels, setup.log, setup.snapshots])) {
        rates.append(h("li", "", t("wiz.rate", {app: appOf(k), n: perMinute(k, setup.pace) * setup.bots})));
      }
      return [
        h("p", "lede", t("wiz.bots_lede")),
        field(t("wiz.bots"), choices(t("wiz.bots"), numbers(MAX_BOTS), setup.bots, set("bots")), t("wiz.bots_note")),
        field(t("wiz.pace"), choices(t("wiz.pace"), pace, setup.pace, set("pace")), t(`wiz.pace_${setup.pace}_note`)),
        rates,
      ];
    },
    review() {
      const text = h("textarea", "toml");
      text.wrap = "off";
      Object.assign(text, {spellcheck: false, value: edited ?? tomlOf(setup)});
      text.setAttribute("aria-label", "pigeonhole.toml");
      text.addEventListener("input", () => {
        edited = text.value;
      });
      return [h("p", "lede", t("wiz.review_lede")), text, h("p", "error", "")];
    },
  };

  function render() {
    const name = STEPS[step];
    panel.title.textContent = t(`wiz.title_${name}`);
    panel.sub.textContent = t("wiz.step", {n: step + 1, of: STEPS.length});
    const progress = h("ol", "wiz-steps");
    STEPS.forEach((x, i) => {
      const li = h("li", i === step ? "now" : i < step ? "done" : "", t(`wiz.short_${x}`));
      if (i === step) li.setAttribute("aria-current", "step");
      progress.append(li);
    });
    const body = steps[name]();
    const nav = h("div", "row wiz-nav");
    if (step > 0) {
      nav.append(
        button("btn", t("wiz.back"), () => {
          step--;
          render();
        }),
      );
    }
    const last = step === STEPS.length - 1;
    const next = button("btn primary", last ? t("wiz.start") : t("wiz.next"), async () => {
      if (!last) {
        step++;
        if (STEPS[step] === "review") edited = null;
        render();
        return;
      }
      const [text, error] = [panel.body.querySelector("textarea"), panel.body.querySelector(".error")];
      next.disabled = true;
      error.textContent = "";
      const failed = await on.start(text.value);
      if (failed) {
        error.textContent = failed;
        next.disabled = false;
      }
    });
    nav.append(next);
    panel.body.replaceChildren(progress, ...body, nav);
    on.sketch(sketch(setup, name));
  }

  return {render};
}
