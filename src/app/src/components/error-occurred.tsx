import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyTitle } from "#/components/ui/empty";

export default function ErrorOccurred({ error }: { error: Error }) {
  return (
    <div className={"flex size-full items-center justify-center"}>
      <Empty>
        <EmptyHeader>
          <EmptyTitle>Critical Error</EmptyTitle>
          <EmptyDescription>An unexpected error occurred.</EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <p className={"font-mono"}>{error.message}</p>
        </EmptyContent>
      </Empty>
    </div>
  );
}
