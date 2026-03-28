import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/(public)/auth/')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div>Hello "/(public)/auth/"!</div>
}
