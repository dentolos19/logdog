import { expect, test } from "bun:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { DataTable } from "../src/components/ui/data-table";

test("Table v9 retains local pagination and escapes cell content", () => {
  const html = renderToStaticMarkup(
    createElement(DataTable, {
      columns: [{ accessorKey: "name", header: "Name" }],
      data: Array.from({ length: 12 }, (_, index) => ({ name: index === 0 ? "<script>" : `Record ${index}` })),
    }),
  );
  expect(html).toContain("&lt;script&gt;");
  expect(html).toContain("Record 9");
  expect(html).not.toContain("Record 10");
  expect(html).toContain("of 2");
});

test("Table v9 retains empty results", () => {
  const html = renderToStaticMarkup(
    createElement(DataTable, {
      columns: [{ accessorKey: "name", header: "Name" }],
      data: [],
    }),
  );
  expect(html).toContain("No results.");
});

test("Table v9 applies controlled sorting and filtering", () => {
  const html = renderToStaticMarkup(
    createElement(DataTable, {
      columns: [{ accessorKey: "name", header: "Name" }],
      data: [{ name: "Alpha One" }, { name: "Beta" }, { name: "Alpha Two" }],
      state: { sorting: [{ id: "name", desc: true }], columnFilters: [{ id: "name", value: "Alpha" }] },
    }),
  );
  expect(html).not.toContain(">Beta<");
  expect(html.indexOf("Alpha Two")).toBeLessThan(html.indexOf("Alpha One"));
});
