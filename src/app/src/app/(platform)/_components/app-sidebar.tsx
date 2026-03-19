"use client";

import { isDevelopment } from "@/environment";
import { useAuth } from "@/components/auth-provider";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { ChevronUpIcon, FlaskConicalIcon, LayoutDashboardIcon, LogOutIcon, ScrollTextIcon, SettingsIcon } from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";

const BASE_NAV_ITEMS = [
  { label: "Dashboard", href: "/dashboard", icon: LayoutDashboardIcon },
  { label: "Logs", href: "/logs", icon: ScrollTextIcon },
];

const DEV_NAV_ITEMS = [
  { label: "Development", href: "/development", icon: FlaskConicalIcon },
];

const NAV_ITEMS = isDevelopment ? [...BASE_NAV_ITEMS, ...DEV_NAV_ITEMS] : BASE_NAV_ITEMS;

export function AppSidebar() {
  const { user, signOut } = useAuth();
  const pathname = usePathname();
  const router = useRouter();

  const handleSignOut = async () => {
    await signOut();
    router.push("/auth");
  };

  const avatarInitials = user?.email?.slice(0, 2).toUpperCase() ?? "??";

  return (
    <Sidebar>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map((item) => (
                <SidebarMenuItem key={item.href}>
                  <SidebarMenuButton asChild={true} isActive={pathname === item.href}>
                    <Link href={item.href}>
                      <item.icon />
                      <span>{item.label}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <DropdownMenu>
              <DropdownMenuTrigger asChild={true}>
                <SidebarMenuButton size={"lg"}>
                  <Avatar size={"sm"}>
                    <AvatarFallback>{avatarInitials}</AvatarFallback>
                  </Avatar>
                  <span className={"flex-1 truncate text-sm"}>{user?.email}</span>
                  <ChevronUpIcon className={"ml-auto"} />
                </SidebarMenuButton>
              </DropdownMenuTrigger>
              <DropdownMenuContent side={"top"} align={"start"} className={"w-56"}>
                <DropdownMenuLabel className={"truncate"}>{user?.email}</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem>
                  <SettingsIcon />
                  Account
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem variant={"destructive"} onSelect={handleSignOut}>
                  <LogOutIcon />
                  Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}
