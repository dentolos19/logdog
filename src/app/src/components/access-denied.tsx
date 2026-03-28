import { Link } from "@tanstack/react-router";
import { Button } from "#/components/ui/button";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyTitle } from "#/components/ui/empty";

export default function AccessDenied() {
  return (
    <div className={"flex size-full items-center justify-center"}>
      <Empty>
        <EmptyHeader>
          <EmptyTitle>Access Denied</EmptyTitle>
          <EmptyDescription>You do not have permission to access this resource.</EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <Button asChild>
            <Link to={"/"}>Home</Link>
          </Button>
        </EmptyContent>
      </Empty>
    </div>
  );
}
