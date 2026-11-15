// Teste da tela do MVP (opcional, requer Node.js): cd tests/js && npm i jsdom@24 && node teste_tela_mvp.js
const { JSDOM } = require("jsdom");
const fs = require("fs");
const html = fs.readFileSync(require("path").join(__dirname, "../../mvp_demo/index.html"), "utf8");
const dados = fs.readFileSync(require("path").join(__dirname, "../../mvp_demo/data/barreiras_mvp.json"), "utf8");
const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://localhost/",
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(dados)) }); } });
setTimeout(() => {
  const d = dom.window.document, erros = [];
  const total = d.getElementById("r-total").textContent, alto = d.getElementById("r-alto").textContent;
  const itens = d.querySelectorAll("#lista li").length, pontos = d.querySelectorAll("#mapa circle").length;
  console.log({ total, alto, conf: d.getElementById("r-conf").textContent,
                resolv: d.getElementById("r-resolv").textContent, itens, pontos,
                primeiro: d.querySelector("#lista li strong").textContent });
  const sel = d.getElementById("f-origem"); sel.value = "sp156"; sel.dispatchEvent(new dom.window.Event("change"));
  console.log("filtro sp156 ->", d.querySelectorAll("#lista li").length, "itens;", d.getElementById("anuncio").textContent);
  if (+total !== itens) erros.push("lista != total");
  if (pontos !== itens + 1) erros.push("pontos no mapa != itens + estação");
  console.log(erros.length ? "FALHOU: " + erros : "TELA OK");
}, 300);
